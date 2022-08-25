package spanner

import (
	"context"
	"fmt"
	"github.com/bitleak/lmstfy/datamanager/storage/model"
	"regexp"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc/codes"

	"github.com/bitleak/lmstfy/config"
)

var cfg = &config.SpannerConfig{
	Project:   "test-project",
	Instance:  "test-instance",
	Database:  "test-db",
	TableName: "lmstfy_jobs",
}

var (
	poolName = "default"
	jobIDs   = []string{"1", "2", "3"}
	ctx      = context.Background()
)

func CreateInstance(ctx context.Context, uri string) error {
	matches := regexp.MustCompile("projects/(.*)/instances/(.*)/databases/.*").FindStringSubmatch(uri)
	if matches == nil || len(matches) != 3 {
		return fmt.Errorf("invalid instance id %s", uri)
	}
	instanceName := "projects/" + matches[1] + "/instances/" + matches[2]

	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer instanceAdminClient.Close()

	_, err = instanceAdminClient.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: instanceName,
	})
	if err != nil && spanner.ErrCode(err) != codes.NotFound {
		return err
	}
	if err == nil {
		return nil
	}

	_, err = instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + matches[1],
		InstanceId: matches[2],
	})
	if err != nil {
		return err
	}
	return nil
}

func CreateDatabase(ctx context.Context, uri string) error {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(uri)
	if matches == nil || len(matches) != 3 {
		return fmt.Errorf("invalid database id %s", uri)
	}

	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	_, err = databaseAdminClient.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: uri})
	if err != nil && spanner.ErrCode(err) != codes.NotFound {
		return err
	}
	if err == nil {
		// db exists
		return nil
	}

	op, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          matches[1],
		CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
		ExtraStatements: []string{
			`CREATE TABLE lmstfy_jobs (
								pool_name    STRING(1024),
								job_id       STRING(1024),
								namespace    STRING(1024),
								queue        STRING(1024),
								body         BYTES(MAX),
								expired_time INT64 NOT NULL,
								ready_time   INT64 NOT NULL,
								tries        INT64 NOT NULL,
                                created_time  INT64 NOT NULL
			                       ) PRIMARY KEY (job_id)`,
		},
	})
	if err != nil {
		return err
	}
	if _, err = op.Wait(ctx); err != nil {
		return err
	}
	return nil
}

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
