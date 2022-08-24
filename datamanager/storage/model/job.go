package model

type JobData struct {
	JobID       string `spanner:"job_id" json:"job_id"`
	Namespace   string `spanner:"namespace" json:"namespace"`
	Queue       string `spanner:"queue" json:"queue"`
	Body        []byte `spanner:"body" json:"body"`
	ExpiredTime int64  `spanner:"expired_time" json:"expired_time"`
	ReadyTime   int64  `spanner:"ready_time" json:"ready_time"`
	Tries       int64  `spanner:"tries" json:"tries"`
	CreatedTime int64  `spanner:"created_time" json:"created_time"`
}

type JobDataReq struct {
	Namespace string
	Queue     string
	ReadyTime int64
	Count     int64
}
