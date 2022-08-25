package pumper

import (
	"context"
	"errors"
	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/datamanager/storage"
	"github.com/bitleak/lmstfy/helper"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/sirupsen/logrus"
	"time"

	"github.com/bitleak/lmstfy/datamanager/storage/model"
	"github.com/bitleak/lmstfy/engine"
	"github.com/go-redsync/redsync/v4"
)

const (
	DefaultStoragePumpPeriod = 10 * 60 // number of seconds, equals 10 minutes
	LoopPumpLockName         = "LP_redsync"
	MaxJobBatchSize          = 1000
)

type Pumper interface {
	// LoopPump periodically checks job data ready time and pumps jobs to engine
	LoopPump()

	SetPumpPeriod(period int64)
	Shutdown()
}

type DefaultPumper struct {
	storage     storage.Storage
	engine      engine.Engine
	redisClient *redis.Client
	shutdown    chan struct{}
	pumpPeriod  int64
}

func NewDefaultPumper(cfg *config.Config, stor storage.Storage, eng engine.Engine) (Pumper, error) {
	if cfg == nil {
		return nil, errors.New("failed to create new pumper, nil config")
	}
	client := helper.NewRedisClient(&cfg.AdminRedis, nil)
	if client.Ping(context.Background()).Err() != nil {
		return nil, errors.New("failed to create redis client for pumper")
	}
	return &DefaultPumper{
		storage:     stor,
		engine:      eng,
		redisClient: client,
		shutdown:    make(chan struct{}),
		pumpPeriod:  int64(DefaultStoragePumpPeriod),
	}, nil
}

// LoopPump periodically checks job data ready time and pumps jobs to engine
func (dp *DefaultPumper) LoopPump() {
	tick := time.NewTicker(time.Duration(dp.pumpPeriod) * time.Second)
	//mutex := newLoopPumpMutex(mgr.redisClient)
	pool := goredis.NewPool(dp.redisClient)
	rs := redsync.New(pool)
	mutex := rs.NewMutex(LoopPumpLockName)
	for {
		select {
		case now := <-tick.C:
			ctx := context.Background()
			if err := mutex.LockContext(ctx); err != nil {
				logrus.Errorf("LoopPump failed to acquire lock: %v", err)
				continue
			}

			req := &model.JobDataReq{
				PoolName:  dp.engine.GetPoolName(),
				ReadyTime: now.Unix() + dp.pumpPeriod,
				Count:     MaxJobBatchSize,
			}
			jobs, err := dp.storage.GetReadyJobs(ctx, req)
			if err != nil {
				logrus.Errorf("LoopPump failed to GetReadyJobs: %v", err)
				continue
			}
			jobsID := make([]string, 0)
			for _, j := range jobs {
				_, err := dp.engine.Publish(j.Namespace, j.Queue, j.Body, uint32(j.ExpiredTime),
					uint32(j.ReadyTime-now.Unix()), uint16(j.Tries))
				if err != nil {
					logrus.Errorf("LoopPump failed to publish job:%v with error %v", j.JobID, err)
					continue
				}
				jobsID = append(jobsID, j.JobID)
			}

			_, err = dp.storage.DelJobs(ctx, jobsID)
			if err != nil {
				logrus.Errorf("LoopPump delete jobs failed:%v", err)
			}
			if ok, err := mutex.UnlockContext(ctx); !ok || err != nil {
				logrus.Errorf("LoopPump failed to release lock: %v", err)
				continue
			}
		case <-dp.shutdown:
			return
		}
	}
}

func (dp *DefaultPumper) Shutdown() {
	close(dp.shutdown)
}

func (dp *DefaultPumper) SetPumpPeriod(period int64) {
	if period <= 0 {
		return
	}
	dp.pumpPeriod = period
}
