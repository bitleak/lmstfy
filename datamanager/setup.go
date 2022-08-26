package datamanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/datamanager/lock"
	"github.com/bitleak/lmstfy/datamanager/pumper"
	"github.com/bitleak/lmstfy/datamanager/storage"
	"github.com/bitleak/lmstfy/datamanager/storage/model"
	"github.com/bitleak/lmstfy/datamanager/storage/spanner"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/helper"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

const (
	maxJobBatchSize = 128
)

type DataManager struct {
	pools   map[string]engine.Engine
	pumpers map[string]pumper.Pumper

	mu       sync.Mutex
	redisCli *redis.Client
	storage  storage.Storage
}

var dataManager *DataManager

func Init(cfg *config.Config) (err error) {
	dataManager, err = NewDataManger(cfg)
	return err
}

func Get() *DataManager {
	return dataManager
}

func NewDataManger(cfg *config.Config) (*DataManager, error) {
	if cfg.SecondaryStorage == nil {
		return nil, errors.New("nil second storage config")
	}
	storage, err := spanner.NewStorage(cfg.SecondaryStorage)
	if err != nil {
		return nil, err
	}
	redisCli := helper.NewRedisClient(&cfg.AdminRedis, nil)
	if redisCli.Ping(context.Background()).Err() != nil {
		return nil, fmt.Errorf("create redis client err: %w", err)
	}
	return &DataManager{
		redisCli: redisCli,
		storage:  storage,
		pools:    make(map[string]engine.Engine),
		pumpers:  make(map[string]pumper.Pumper),
	}, nil
}

func (m *DataManager) PumpFn(name string, pool engine.Engine) func() bool {
	return func() bool {
		now := time.Now()
		req := &model.JobDataReq{
			PoolName: name,
			// FIXME: don't hard code deadline here
			ReadyTime: now.Unix() + 3600,
			Count:     maxJobBatchSize,
		}
		ctx := context.TODO()
		jobs, err := m.storage.GetReadyJobs(ctx, req)
		if err != nil {
			logrus.Errorf("Get ready jobs err: %v", err)
			return false
		}
		jobsID := make([]string, 0)
		for _, j := range jobs {
			_, err := pool.Publish(j.Namespace, j.Queue, j.Body, uint32(j.ExpiredTime),
				uint32(j.ReadyTime-now.Unix()), uint16(j.Tries))
			if err != nil {
				logrus.Errorf("publish job:%v with error %v", j.JobID, err)
				return false
			}
			jobsID = append(jobsID, j.JobID)
		}

		if _, err := m.storage.DelJobs(ctx, jobsID); err != nil {
			logrus.Errorf("LoopPump delete jobs failed:%v", err)
			return false
		}

		return len(jobsID) == maxJobBatchSize
	}
}

func (m *DataManager) AddPool(name string, pool engine.Engine) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// FIXME: choose a right expiry value
	redisLock := lock.NewRedisLock(m.redisCli, name, 10*time.Second)
	pumper := pumper.NewDefault(redisLock, time.Minute)
	go pumper.Loop(m.PumpFn(name, pool))
}

func (m *DataManager) AddJob(ctx context.Context, job *model.JobData) error {
	return m.storage.BatchAddJobs(ctx, []*model.JobData{job})
}

func (m *DataManager) Shutdown() {
	// Stop and release pumper here
}
