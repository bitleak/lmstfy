package spanner

import (
	"context"
	"time"

	"github.com/bitleak/lmstfy/storage/persistence/model"
)

var (
	poolName = "default"
	jobIDs   = []string{"1", "2", "3"}
	ctx      = context.Background()
)

func createTestJobsData() []*model.JobData {
	jobs := make([]*model.JobData, 0)
	j1 := &model.JobData{
		PoolName:    poolName,
		JobID:       "1",
		Namespace:   "n1",
		Queue:       "q1",
		Body:        []byte("hello_j1"),
		ExpiredTime: time.Now().Unix() + 120,
		ReadyTime:   time.Now().Unix() + 30,
		Tries:       1,
		CreatedTime: time.Now().Unix(),
	}
	j2 := &model.JobData{
		PoolName:    poolName,
		JobID:       "2",
		Namespace:   "n1",
		Queue:       "q2",
		Body:        []byte("hello_j2"),
		ExpiredTime: time.Now().Unix() + 120,
		ReadyTime:   time.Now().Unix() + 60,
		Tries:       1,
		CreatedTime: time.Now().Unix(),
	}
	j3 := &model.JobData{
		PoolName:    poolName,
		JobID:       "3",
		Namespace:   "n1",
		Queue:       "q1",
		Body:        []byte("hello_j3"),
		ExpiredTime: time.Now().Unix() + 120,
		ReadyTime:   time.Now().Unix() + 90,
		Tries:       1,
		CreatedTime: time.Now().Unix(),
	}
	jobs = append(jobs, j1, j2, j3)
	return jobs
}

func createTestReqData() []*model.JobDataReq {
	req := make([]*model.JobDataReq, 0)
	r1 := &model.JobDataReq{
		PoolName:  poolName,
		Namespace: "n1",
		Queue:     "q1",
		ReadyTime: 0,
		Count:     10,
	}
	r2 := &model.JobDataReq{
		PoolName:  poolName,
		Namespace: "n1",
		Queue:     "q2",
		ReadyTime: 0,
		Count:     10,
	}
	req = append(req, r1, r2)
	return req
}

func createTestReqData2() *model.JobDataReq {
	req := &model.JobDataReq{
		PoolName:  poolName,
		ReadyTime: time.Now().Unix() + 80,
		Count:     10,
	}
	return req
}
