package spanner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/go-redis/redis/v8"
	"google.golang.org/api/option"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/storage/persistence/model"
)

const (
	MaxJobBatchSize = 1000
)

type Spanner struct {
	cli         *spanner.Client
	redisClient *redis.Client
	tableName   string
}

func createSpannerClient(cfg *config.SpannerConfig) (*spanner.Client, error) {
	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.Project, cfg.Instance, cfg.Database)
	if cfg.CredentialsFile != "" {
		opt := option.WithCredentialsFile(cfg.CredentialsFile)
		return spanner.NewClient(context.Background(), db, opt)
	}
	return spanner.NewClient(context.Background(), db)
}

func NewSpanner(cfg *config.SpannerConfig) (*Spanner, error) {
	client, err := createSpannerClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Spanner{
		cli:       client,
		tableName: cfg.TableName,
	}, nil
}

// BatchAddJobs write jobs data into secondary storage
func (s *Spanner) BatchAddJobs(ctx context.Context, jobs []engine.Job) (err error) {
	err = validateReq(jobs)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	dbJobs := make([]*model.DBJob, 0)
	for _, job := range jobs {
		j := &model.DBJob{
			PoolName:    job.Pool(),
			JobID:       job.ID(),
			Namespace:   job.Namespace(),
			Queue:       job.Queue(),
			Body:        job.Body(),
			ExpiredTime: now + int64(job.TTL()),
			ReadyTime:   now + int64(job.Delay()),
			Tries:       int64(job.Tries()),
			CreatedTime: now,
		}
		dbJobs = append(dbJobs, j)
	}

	_, err = s.cli.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutations := make([]*spanner.Mutation, 0)
		for _, job := range dbJobs {
			mut, err := spanner.InsertStruct(s.tableName, job)
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
func (s *Spanner) BatchGetJobs(ctx context.Context, req []*model.DBJobReq) (jobs []engine.Job, err error) {
	txn := s.cli.ReadOnlyTransaction()
	now := time.Now().Unix()
	defer txn.Close()

	for _, r := range req {
		iter := txn.Query(ctx, spanner.Statement{
			SQL: "SELECT pool_name, job_id, namespace, queue, body, ready_time, expired_time, created_time, tries " +
				"FROM lmstfy_jobs WHERE pool_name = @poolname and namespace = @namespace and queue = @queue and ready_time >= @readytime LIMIT @limit",
			Params: map[string]interface{}{
				"poolname":  r.PoolName,
				"namespace": r.Namespace,
				"queue":     r.Queue,
				"readytime": r.ReadyTime,
				"limit":     r.Count,
			},
		})
		err = iter.Do(func(row *spanner.Row) error {
			elem := &model.DBJob{}
			if err = row.ToStruct(elem); err != nil {
				return err
			}
			j := engine.NewJobWithoutMarshal(elem.Namespace, elem.Queue, elem.Body, uint32(elem.ExpiredTime),
				uint32(elem.ReadyTime-now), uint16(elem.Tries), elem.JobID)
			jobs = append(jobs, j)
			return nil
		})

		if err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

// GetQueueSize returns the size of data in storage which are due before certain due time
func (s *Spanner) GetQueueSize(ctx context.Context, req []*model.DBJobReq) (count map[string]int64, err error) {
	txn := s.cli.ReadOnlyTransaction()
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
func (s *Spanner) DelJobs(ctx context.Context, jobIDs []string) (count int64, err error) {
	if len(jobIDs) == 0 {
		return 0, nil
	}
	_, err = s.cli.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
func (s *Spanner) GetReadyJobs(ctx context.Context, req *model.DBJobReq) (jobs []engine.Job, err error) {
	if req.ReadyTime <= 0 {
		return nil, fmt.Errorf("GetReadyJobs failed: missing readytime parameter")
	}
	now := time.Now().Unix()
	txn := s.cli.ReadOnlyTransaction()
	defer txn.Close()
	iter := txn.Query(ctx, spanner.Statement{
		SQL: "SELECT pool_name, job_id, namespace, queue, body, ready_time, expired_time, created_time, tries " +
			"FROM lmstfy_jobs WHERE pool_name = @poolname and ready_time <= @readytime LIMIT @limit",
		Params: map[string]interface{}{
			"poolname":  req.PoolName,
			"readytime": req.ReadyTime,
			"limit":     req.Count,
		},
	})
	err = iter.Do(func(row *spanner.Row) error {
		elem := &model.DBJob{}
		if err = row.ToStruct(elem); err != nil {
			return err
		}
		j := engine.NewJobWithoutMarshal(elem.Namespace, elem.Queue, elem.Body, uint32(elem.ExpiredTime),
			uint32(elem.ReadyTime-now), uint16(elem.Tries), elem.JobID)
		jobs = append(jobs, j)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// BatchGetJobsByID returns job data by job ID
func (s *Spanner) BatchGetJobsByID(ctx context.Context, IDs []string) (jobs []engine.Job, err error) {
	txn := s.cli.ReadOnlyTransaction()
	now := time.Now().Unix()
	defer txn.Close()

	iter := txn.Query(ctx, spanner.Statement{
		SQL: "SELECT pool_name, job_id, namespace, queue, body, ready_time, expired_time, created_time, tries " +
			"FROM lmstfy_jobs WHERE job_id IN UNNEST(@ids) LIMIT @limit",
		Params: map[string]interface{}{
			"ids":   IDs,
			"limit": len(IDs),
		},
	})
	err = iter.Do(func(row *spanner.Row) error {
		elem := &model.DBJob{}
		if err = row.ToStruct(elem); err != nil {
			return err
		}
		j := engine.NewJobWithoutMarshal(elem.Namespace, elem.Queue, elem.Body, uint32(elem.ExpiredTime),
			uint32(elem.ReadyTime-now), uint16(elem.Tries), elem.JobID)
		jobs = append(jobs, j)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

func validateReq(req []engine.Job) error {
	if len(req) == 0 {
		return errors.New("invalid req, null jobs list")
	}
	if len(req) > MaxJobBatchSize {
		return errors.New("invalid req, exceed maximum input batch size")
	}
	return nil
}

func (s *Spanner) Close() {
	s.cli.Close()
}
