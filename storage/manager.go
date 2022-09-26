package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/helper"
	"github.com/bitleak/lmstfy/log"
	"github.com/bitleak/lmstfy/storage/lock"
	"github.com/bitleak/lmstfy/storage/persistence/model"
	"github.com/bitleak/lmstfy/storage/persistence/spanner"
	"github.com/bitleak/lmstfy/storage/pumper"
)

const (
	defaultMaxJobPumpBatchSize = 512

	defaultLockExpiry   = 15 * time.Second
	defaultPumpInterval = 3 * time.Second

	addJobSuccessStatus = "success"
	addJobFailedStatus  = "failed"

	redisMemoryUsageWatermark = 0.6 //  used_memory / max_memory
)

type Manager struct {
	wg      sync.WaitGroup
	mu      sync.Mutex
	pools   map[string]engine.Engine
	pumpers map[string]pumper.Pumper

	redisCli         *redis.Client
	storage          Persistence
	maxPumpBatchSize int64
}

var manager *Manager

func Init(cfg *config.Config) (err error) {
	manager, err = NewManger(cfg)
	return err
}

func Get() *Manager {
	return manager
}

func createPersistStorage(cfg *config.SecondaryStorage) (Persistence, error) {
	if cfg.Spanner != nil {
		return spanner.NewSpanner(cfg.Spanner)
	}
	return nil, errors.New("require at least one of [Spanner]")
}

func NewManger(cfg *config.Config) (*Manager, error) {
	if cfg.SecondaryStorage == nil {
		return nil, errors.New("nil second storage config")
	}
	storage, err := createPersistStorage(cfg.SecondaryStorage)
	if err != nil {
		return nil, err
	}
	redisCli := helper.NewRedisClient(&cfg.AdminRedis, nil)
	if redisCli.Ping(context.Background()).Err() != nil {
		return nil, fmt.Errorf("create redis client err: %w", err)
	}
	return &Manager{
		redisCli:         redisCli,
		storage:          storage,
		pools:            make(map[string]engine.Engine),
		pumpers:          make(map[string]pumper.Pumper),
		maxPumpBatchSize: cfg.SecondaryStorage.MaxJobPumpBatchSize,
	}, nil
}

func (m *Manager) PumpFn(name string, pool engine.Engine, threshold int64) func() bool {
	return func() bool {
		logger := log.Get().WithField("pool", name)
		if isHighRedisMemUsage(m.redisCli) {
			logger.Error("High redis usage, storage stops pumping data")
			return false
		}

		if m.maxPumpBatchSize == 0 || m.maxPumpBatchSize > defaultMaxJobPumpBatchSize {
			m.maxPumpBatchSize = defaultMaxJobPumpBatchSize
		}
		batchSize := strconv.Itoa(int(m.maxPumpBatchSize))
		now := time.Now()
		req := &model.JobDataReq{
			PoolName:  name,
			ReadyTime: now.Unix() + threshold,
			Count:     m.maxPumpBatchSize,
		}
		ctx := context.TODO()
		jobs, err := m.storage.GetReadyJobs(ctx, req)
		if err != nil {
			logger.WithError(err).Errorf("Failed to get ready jobs from storage")
			return false
		}

		if len(jobs) == 0 {
			return false
		}
		metrics.pumperGetJobLatency.WithLabelValues(batchSize).Observe(float64(time.Since(now).Milliseconds()))
		logger.Debugf("Got %d ready jobs from storage", len(jobs))

		startPublishTime := time.Now()
		jobsID := make([]string, 0)
		for _, job := range jobs {
			j := engine.NewJob(job.Namespace, job.Queue, job.Body, uint32(job.ExpiredTime),
				uint32(job.ReadyTime-now.Unix()), uint16(job.Tries), job.JobID)
			_, err := pool.Publish(j)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"job": j,
					"err": err,
				}).Errorf("Failed to publish job")
				continue
			}
			jobsID = append(jobsID, job.JobID)
		}
		metrics.pumperPubJobLatency.WithLabelValues(batchSize).Observe(float64(time.Since(startPublishTime).Milliseconds()))

		startDelJobsTime := time.Now()
		if _, err := m.storage.DelJobs(ctx, jobsID); err != nil {
			logger.WithFields(logrus.Fields{
				"jobs": jobsID,
				"err":  err,
			}).Errorf("Failed to delete jobs from storage")
			return false
		}
		metrics.storageDelJobs.WithLabelValues(name).Add(float64(len(jobsID)))
		metrics.pumperDelJobLatency.WithLabelValues(batchSize).Observe(float64(time.Since(startDelJobsTime).Milliseconds()))
		return int64(len(jobsID)) == m.maxPumpBatchSize
	}
}

func (m *Manager) AddPool(name string, pool engine.Engine, threshold int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	redisLock := lock.NewRedisLock(m.redisCli, name, defaultLockExpiry)
	pumper := pumper.NewDefault(redisLock, defaultPumpInterval)
	m.pumpers[name] = pumper

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		pumper.Loop(m.PumpFn(name, pool, threshold))
	}()
}

func (m *Manager) AddJob(ctx context.Context, job *model.JobData) error {
	var status string
	err := m.storage.BatchAddJobs(ctx, []*model.JobData{job})
	if err == nil {
		status = addJobSuccessStatus
	} else {
		status = addJobFailedStatus
	}
	metrics.storageAddJobs.WithLabelValues(job.PoolName, job.Namespace, job.Queue, status).Inc()
	return err
}

func (m *Manager) GetJobByID(ctx context.Context, ID string) ([]*model.JobData, error) {
	return m.storage.BatchGetJobsByID(ctx, []string{ID})
}

func (m *Manager) Shutdown() {
	if m == nil {
		return
	}

	for _, pumper := range m.pumpers {
		pumper.Shutdown()
	}
	m.wg.Wait()

	_ = m.redisCli.Close()
	m.storage.Close()
}

func isHighRedisMemUsage(cli *redis.Client) bool {
	memoryInfo, err := cli.Info(context.TODO(), "memory").Result()
	if err != nil {
		return false
	}
	var usedMem, maxMem int64
	lines := strings.Split(memoryInfo, "\r\n")
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "used_memory":
			usedMem, _ = strconv.ParseInt(fields[1], 10, 64)
		case "maxmemory":
			maxMem, _ = strconv.ParseInt(fields[1], 10, 64)
		default:
			continue
		}
	}
	if maxMem == 0 {
		return false
	}
	return float64(usedMem)/float64(maxMem) > redisMemoryUsageWatermark
}
