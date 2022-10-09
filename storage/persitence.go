package storage

import (
	"context"

	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/storage/persistence/model"
)

// Persistence handles requests related to secondary storage
type Persistence interface {
	// BatchAddJobs write jobs data into secondary storage
	BatchAddJobs(ctx context.Context, jobs []engine.Job, poolName string) (err error)
	// BatchGetJobs pumps data that are due before certain due time
	BatchGetJobs(ctx context.Context, req []*model.DBJobReq) (jobs []engine.Job, err error)
	// GetQueueSize returns the size of data in storage which are due before certain due time
	GetQueueSize(ctx context.Context, req []*model.DBJobReq) (count map[string]int64, err error)
	// DelJobs remove job data from storage based on job id
	DelJobs(ctx context.Context, jobIDs []string) (count int64, err error)
	// GetReadyJobs return jobs which are ready based on input ready time from data storage
	GetReadyJobs(ctx context.Context, req *model.DBJobReq) (jobs []engine.Job, err error)
	// BatchGetJobsByID returns job data by job ID
	BatchGetJobsByID(ctx context.Context, IDs []string) (jobs []engine.Job, err error)
	Close()
}
