package storage

import (
	"context"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/storage/model"
)

// DataManager handles data related request between engine and data storage
type DataManager interface {
	// BatchAddJobs write jobs data into secondary storage
	BatchAddJobs(ctx context.Context, jobs []*model.JobData) (err error)
	// BatchGetJobs pumps data that are due before certain due time
	BatchGetJobs(ctx context.Context, req []*model.JobDataReq) (jobs []*model.JobData, err error)
	// GetQueueSize returns the size of data in storage which are due before certain due time
	GetQueueSize(ctx context.Context, req []*model.JobDataReq) (count map[string]int64, err error)
	// DelJobs remove job data from storage based on job id
	DelJobs(ctx context.Context, jobIDs []string) (count int64, err error)
	// GetReadyJobs return jobs which are ready based on input ready time from data storage
	GetReadyJobs(ctx context.Context, req *model.JobDataReq) (jobs []*model.JobData, err error)
	// LoopPump periodically checks job data ready time and pumps jobs to engine
	LoopPump(eng engine.Engine)

	ShutDown()
}
