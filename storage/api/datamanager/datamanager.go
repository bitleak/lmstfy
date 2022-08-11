package datamanager

// DataManager handles data related request between engine and data storage
type DataManager interface {
	// PumpIn pumps data into storage
	PumpIn(jobs []*JobData) (count int64, err error)
	// PumpOut pumps data which are due before certain due time
	PumpOut(req []*DataRequest, limit int64) (jobs []*JobData, err error)
	// GetStorageSize returns the size of data in storage which are due before certain due time
	GetStorageSize(req []*DataRequest) (count int64, err error)
	// DelByID remove job data from storage based on job id
	DelByID(jobIDs []string) (count int64, err error)
	// DelBeforeDue remove job which are due before certain due time from storage
	DelBeforeDue(req []*DataRequest) (count int64, err error)
	// DelBehindDue remove job which are due after certain due time from storage
	DelBehindDue(req []*DataRequest) (count int64, err error)
}
