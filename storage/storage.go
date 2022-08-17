package storage

import (
	"context"

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
}
