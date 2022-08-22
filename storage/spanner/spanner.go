package spanner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	log "github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/storage"
	"github.com/bitleak/lmstfy/storage/model"
)

const (
	MaxJobBatchSize       = 1000
	DefaultLoopPumpPeriod = 10 * time.Second
	LoopPumpLockName      = "LP_redsync"
)

type SpannerDataMgr struct {
	cli         *spanner.Client
	redisClient *redis.Client
	tableName   string
	shutdown    chan struct{}
}

func NewDataMgr(cfg *config.SpannerConfig, redisClient *redis.Client) (storage.DataManager, error) {
	client, err := CreateSpannerClient(cfg)
	if err != nil {
		return nil, err
	}
	return &SpannerDataMgr{
		cli:         client,
		redisClient: redisClient,
		tableName:   cfg.TableName,
		shutdown:    make(chan struct{}),
	}, nil
}

// BatchAddJobs write jobs data into secondary storage
func (mgr *SpannerDataMgr) BatchAddJobs(ctx context.Context, jobs []*model.JobData) (err error) {
	err = validateReq(jobs)
	if err != nil {
		return err
	}

	_, err = mgr.cli.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutations := make([]*spanner.Mutation, 0)
		for _, job := range jobs {
			mut, err := spanner.InsertStruct(mgr.tableName, job)
			if err != nil {
				return err
			}
			mutations = append(mutations, mut)
		}
		return txn.BufferWrite(mutations)
	})
	return err
}

// BatchGetJobs pumps data that are due before certain due time
func (mgr *SpannerDataMgr) BatchGetJobs(ctx context.Context, req []*model.JobDataReq) (jobs []*model.JobData, err error) {
	txn := mgr.cli.ReadOnlyTransaction()
	defer txn.Close()

	for _, r := range req {
		iter := txn.Query(ctx, spanner.Statement{
			SQL: "SELECT job_id, namespace, queue, body, ready_time, expired_time, created_time, tries " +
				"FROM lmstfy_jobs WHERE namespace = @namespace and queue = @queue and ready_time >= @readytime LIMIT @limit",
			Params: map[string]interface{}{
				"namespace": r.Namespace,
				"queue":     r.Queue,
				"readytime": r.ReadyTime,
				"limit":     r.Count,
			},
		})
		err = iter.Do(func(row *spanner.Row) error {
			elem := &model.JobData{}
			if err = row.ToStruct(elem); err != nil {
				return err
			}
			jobs = append(jobs, elem)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

// GetQueueSize returns the size of data in storage which are due before certain due time
func (mgr *SpannerDataMgr) GetQueueSize(ctx context.Context, req []*model.JobDataReq) (count map[string]int64, err error) {
	txn := mgr.cli.ReadOnlyTransaction()
	defer txn.Close()
	count = make(map[string]int64)

	for _, r := range req {
		var tmpCount int64
		key := fmt.Sprintf("%s/%s", r.Namespace, r.Queue)
		readRow := func(r *spanner.Row) error { return r.ColumnByName("tmpCount", &tmpCount) }
		iter := txn.Query(ctx, spanner.Statement{
			SQL: "SELECT COUNT(*) AS tmpCount FROM lmstfy_jobs WHERE namespace = @namespace and queue = @queue and " +
				"ready_time >= @readytime LIMIT 1",
			Params: map[string]interface{}{
				"namespace": r.Namespace,
				"queue":     r.Queue,
				"readytime": r.ReadyTime,
			},
		})
		err = iter.Do(readRow)
		if err != nil {
			continue
		}
		count[key] = tmpCount
	}
	return count, nil
}

// DelJobs remove job data from storage based on job id
func (mgr *SpannerDataMgr) DelJobs(ctx context.Context, jobIDs []string) (count int64, err error) {
	if len(jobIDs) == 0 {
		return 0, nil
	}
	_, err = mgr.cli.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		count, err = txn.Update(ctx, spanner.Statement{
			SQL: "DELETE FROM lmstfy_jobs WHERE job_id IN UNNEST(@ids)",
			Params: map[string]interface{}{
				"ids": jobIDs,
			},
		})
		return err
	})
	return count, err
}

// GetReadyJobs return jobs which are ready based on input ready time from data storage
func (mgr *SpannerDataMgr) GetReadyJobs(ctx context.Context, req *model.JobDataReq) (jobs []*model.JobData, err error) {
	if req.ReadyTime <= 0 {
		return nil, fmt.Errorf("GetReadyJobs failed: missing readytime parameter")
	}
	txn := mgr.cli.ReadOnlyTransaction()
	defer txn.Close()
	iter := txn.Query(ctx, spanner.Statement{
		SQL: "SELECT job_id, namespace, queue, body, ready_time, expired_time, created_time, tries " +
			"FROM lmstfy_jobs WHERE ready_time <= @readytime and ready_time > @nowtime  LIMIT @limit",
		Params: map[string]interface{}{
			"readytime": req.ReadyTime,
			"limit":     req.Count,
			"nowtime":   time.Now().Unix(),
		},
	})
	err = iter.Do(func(row *spanner.Row) error {
		elem := &model.JobData{}
		if err = row.ToStruct(elem); err != nil {
			return err
		}
		jobs = append(jobs, elem)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// LoopPump periodically checks job data ready time and pumps jobs to engine
func (mgr *SpannerDataMgr) LoopPump(eng engine.Engine) {
	tick := time.NewTicker(DefaultLoopPumpPeriod)
	//mutex := newLoopPumpMutex(mgr.redisClient)
	pool := goredis.NewPool(mgr.redisClient)
	rs := redsync.New(pool)
	mutex := rs.NewMutex(LoopPumpLockName)
	for {
		select {
		case now := <-tick.C:
			ctx := context.Background()
			if err := mutex.LockContext(ctx); err != nil {
				log.Errorf("LoopPump failed to acquire lock: %v", err)
				continue
			}

			req := &model.JobDataReq{
				ReadyTime: now.Unix() + int64(DefaultLoopPumpPeriod),
				Count:     MaxJobBatchSize,
			}
			jobs, err := mgr.GetReadyJobs(ctx, req)
			if err != nil {
				log.Errorf("LoopPump failed to GetReadyJobs: %v", err)
				continue
			}
			jobsID := make([]string, 0)
			for _, j := range jobs {
				_, err := eng.Publish(j.Namespace, j.Queue, j.Body, uint32(j.ExpiredTime),
					uint32(j.ReadyTime-now.Unix()), uint16(j.Tries))
				if err != nil {
					log.Errorf("LoopPump failed to publish job:%v with error %v", j.JobID, err)
					continue
				}
				jobsID = append(jobsID, j.JobID)
			}

			_, err = mgr.DelJobs(ctx, jobsID)
			if err != nil {
				log.Errorf("LoopPump delete jobs failed:%v", err)
			}
			time.Sleep(5 * time.Second)
			if ok, err := mutex.UnlockContext(ctx); !ok || err != nil {
				log.Errorf("LoopPump failed to release lock: %v", err)
				continue
			}
		case <-mgr.shutdown:
			return
		}
	}
}

func (mgr *SpannerDataMgr) ShutDown() {
	close(mgr.shutdown)
}

func validateReq(req []*model.JobData) error {
	if len(req) == 0 {
		return errors.New("invalid req, null jobs list")
	}
	if len(req) > MaxJobBatchSize {
		return errors.New("invalid req, exceed maximum input batch size")
	}
	return nil
}

func newLoopPumpMutex(client *redis.Client) *redsync.Mutex {
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)
	mutex := rs.NewMutex("LoopPump-redsync")
	return mutex
}
