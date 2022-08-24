package spanner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/go-redis/redis/v8"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/datamanager/storage"
	"github.com/bitleak/lmstfy/datamanager/storage/model"
)

const (
	MaxJobBatchSize = 1000
)

type SpannerStorage struct {
	cli         *spanner.Client
	redisClient *redis.Client
	tableName   string
}

func NewStorage(cfg *config.SpannerConfig) (storage.Storage, error) {
	client, err := CreateSpannerClient(cfg)
	if err != nil {
		return nil, err
	}
	return &SpannerStorage{
		cli:       client,
		tableName: cfg.TableName,
	}, nil
}

// BatchAddJobs write jobs data into secondary storage
func (mgr *SpannerStorage) BatchAddJobs(ctx context.Context, jobs []*model.JobData) (err error) {
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
func (mgr *SpannerStorage) BatchGetJobs(ctx context.Context, req []*model.JobDataReq) (jobs []*model.JobData, err error) {
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
func (mgr *SpannerStorage) GetQueueSize(ctx context.Context, req []*model.JobDataReq) (count map[string]int64, err error) {
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
func (mgr *SpannerStorage) DelJobs(ctx context.Context, jobIDs []string) (count int64, err error) {
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
func (mgr *SpannerStorage) GetReadyJobs(ctx context.Context, req *model.JobDataReq) (jobs []*model.JobData, err error) {
	if req.ReadyTime <= 0 {
		return nil, fmt.Errorf("GetReadyJobs failed: missing readytime parameter")
	}
	txn := mgr.cli.ReadOnlyTransaction()
	defer txn.Close()
	iter := txn.Query(ctx, spanner.Statement{
		SQL: "SELECT job_id, namespace, queue, body, ready_time, expired_time, created_time, tries " +
			"FROM lmstfy_jobs WHERE ready_time <= @readytime LIMIT @limit",
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

func validateReq(req []*model.JobData) error {
	if len(req) == 0 {
		return errors.New("invalid req, null jobs list")
	}
	if len(req) > MaxJobBatchSize {
		return errors.New("invalid req, exceed maximum input batch size")
	}
	return nil
}
