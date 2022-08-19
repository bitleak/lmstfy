package spanner

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/stretchr/testify/assert"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc/codes"

	"github.com/bitleak/lmstfy/storage/conf"
	"github.com/bitleak/lmstfy/storage/model"
)

var cfg = &conf.SpannerConfig{
	Project:  "test-project",
	Instance: "test-instance",
	Database: "test-db",
}

var (
	tableName = "lmstfy_jobs"
	jobIDs    = []string{"1", "2", "3"}
	ctx       = context.Background()
	db        = "projects/test-project/instances/test-instance/databases/test-db"
)

func init() {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		fmt.Println("failed to find $SPANNER_EMULATOR_HOST value")
		return
	}
	err := createInstance(ctx, db)
	if err != nil {
		fmt.Printf("create instance with error: %v", err)
		return
	}
	err = createDatabase(ctx, db)
	if err != nil {
		fmt.Printf("create db with error: %v", err)
		return
	}
}

func createInstance(ctx context.Context, uri string) error {
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

func createDatabase(ctx context.Context, uri string) error {
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
		JobID:       "1",
		Namespace:   "n1",
		Queue:       "q1",
		Body:        nil,
		ExpiredTime: 1,
		ReadyTime:   1,
		Tries:       1,
		CreatedTime: time.Now().Unix(),
	}
	j2 := &model.JobData{
		JobID:       "2",
		Namespace:   "n1",
		Queue:       "q2",
		Body:        nil,
		ExpiredTime: 1,
		ReadyTime:   1,
		Tries:       1,
		CreatedTime: time.Now().Unix(),
	}
	j3 := &model.JobData{
		JobID:       "3",
		Namespace:   "n1",
		Queue:       "q1",
		Body:        nil,
		ExpiredTime: 1,
		ReadyTime:   1,
		Tries:       1,
		CreatedTime: time.Now().Unix(),
	}
	jobs = append(jobs, j1, j2, j3)
	return jobs
}

func createTestReqData() []*model.JobDataReq {
	req := make([]*model.JobDataReq, 0)
	r1 := &model.JobDataReq{
		Namespace: "n1",
		Queue:     "q1",
		ReadyTime: 0,
		Count:     10,
	}
	r2 := &model.JobDataReq{
		Namespace: "n1",
		Queue:     "q2",
		ReadyTime: 0,
		Count:     10,
	}
	req = append(req, r1, r2)
	return req
}

func TestCreateSpannerClient(t *testing.T) {
	_, err := CreateSpannerClient(cfg)
	assert.Nil(t, err)
}

func TestSpannerDataMgr_BatchAddDelJobs(t *testing.T) {
	cli, err := CreateSpannerClient(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to create spanner client with error: %s", err))
	}
	mgr := NewSpannerDataMgr(cli, tableName)
	jobs := createTestJobsData()
	err = mgr.BatchAddJobs(ctx, jobs)
	if err != nil {
		panic(fmt.Sprintf("Failed to add jobs with error: %s", err))
	}
	t.Logf("add jobs success %v rows", len(jobs))

	count, err := mgr.DelJobs(ctx, jobIDs)
	if err != nil {
		panic(fmt.Sprintf("failed to delete job: %v", err))
	}
	t.Logf("del jobs success %v rows", count)
}

func TestSpannerDataMgr_BatchGetJobs(t *testing.T) {
	cli, err := CreateSpannerClient(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to create spanner client with error: %s", err))
	}
	mgr := NewSpannerDataMgr(cli, tableName)
	jobs := createTestJobsData()
	mgr.BatchAddJobs(ctx, jobs)
	req := createTestReqData()
	_, err = mgr.BatchGetJobs(ctx, req)
	if err != nil {
		panic(fmt.Sprintf("BatchGetJobs failed with error: %s", err))
	}
	mgr.DelJobs(ctx, jobIDs)
}

func TestSpannerDataMgr_GetQueueSize(t *testing.T) {
	cli, err := CreateSpannerClient(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to create spanner client with error: %s", err))
	}
	mgr := NewSpannerDataMgr(cli, tableName)
	jobs := createTestJobsData()
	mgr.BatchAddJobs(ctx, jobs)
	req := createTestReqData()
	count, err := mgr.GetQueueSize(ctx, req)
	if err != nil || len(count) == 0 {
		panic(fmt.Sprintf("BatchGetJobs failed with error: %s", err))
	}
	key1, key2 := fmt.Sprintf("%s/%s", "n1", "q1"), fmt.Sprintf("%s/%s", "n1", "q2")
	assert.EqualValues(t, 2, count[key1])
	assert.EqualValues(t, 1, count[key2])
	mgr.DelJobs(ctx, jobIDs)
}
