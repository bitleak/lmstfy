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

type DefaultPumper struct {
	redisClient       *redis.Client
	shutdown          chan struct{}
	storagePumpPeriod int64
}

func NewPumper(cfg *config.Config, poolConfig *config.RedisConf) (Pumper, error) {
	if cfg == nil {
		return nil, errors.New("failed to create new pumper, nil config")
	}
	client := helper.NewRedisClient(&cfg.AdminRedis, nil)
	if client.Ping(context.Background()).Err() != nil {
		return nil, errors.New("failed to create redis client for pumper")
	}
	period := poolConfig.StoragePumpPeriod
	if period <= 0 {
		period = DefaultStoragePumpPeriod
	}
	return &DefaultPumper{
		redisClient:       client,
		shutdown:          make(chan struct{}),
		storagePumpPeriod: int64(period),
	}, nil
}

// LoopPump periodically checks job data ready time and pumps jobs to engine
func (dp *DefaultPumper) LoopPump(st storage.Storage, eng engine.Engine) {
	tick := time.NewTicker(time.Duration(dp.storagePumpPeriod) * time.Second)
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
				ReadyTime: now.Unix() + dp.storagePumpPeriod,
				Count:     MaxJobBatchSize,
			}
			jobs, err := st.GetReadyJobs(ctx, req)
			if err != nil {
				logrus.Errorf("LoopPump failed to GetReadyJobs: %v", err)
				continue
			}
			jobsID := make([]string, 0)
			for _, j := range jobs {
				_, err := eng.Publish(j.Namespace, j.Queue, j.Body, uint32(j.ExpiredTime),
					uint32(j.ReadyTime-now.Unix()), uint16(j.Tries))
				if err != nil {
					logrus.Errorf("LoopPump failed to publish job:%v with error %v", j.JobID, err)
					continue
				}
				jobsID = append(jobsID, j.JobID)
			}

			_, err = st.DelJobs(ctx, jobsID)
			if err != nil {
				logrus.Errorf("LoopPump delete jobs failed:%v", err)
			}
			time.Sleep(5 * time.Second)
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
