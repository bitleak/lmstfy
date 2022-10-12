package spanner

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc/codes"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/storage/persistence/model"
)

var (
	poolName = "default"
	jobIDs   = []string{"1", "2", "3"}
	ctx      = context.Background()
)

func CreateInstance(ctx context.Context, cfg *config.SpannerConfig) error {
	instanceName := fmt.Sprintf("projects/%s/instances/%s", cfg.Project, cfg.Instance)

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
		Parent:     "projects/" + cfg.Project,
		InstanceId: cfg.Instance,
	})
	return err
}

func CreateDatabase(ctx context.Context, cfg *config.SpannerConfig) error {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	instanceName := fmt.Sprintf("projects/%s/instances/%s", cfg.Project, cfg.Instance)
	dbName := fmt.Sprintf("%s/databases/%s", instanceName, cfg.Database)
	_, err = databaseAdminClient.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: dbName})
	if err != nil && spanner.ErrCode(err) != codes.NotFound {
		return err
	}
	if err == nil {
		// db exists
		return nil
	}

	ddlBytes, err := ioutil.ReadFile("../../../scripts/schemas/spanner/ddls.sql")
	if err != nil {
		return fmt.Errorf("read ddls file: %w", err)
	}
	ddls := make([]string, 0)
	for _, ddl := range strings.Split(string(ddlBytes), ";") {
		ddl = strings.TrimSpace(ddl)
		if len(ddl) != 0 {
			ddls = append(ddls, ddl)
		}
	}
	op, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instanceName,
		CreateStatement: "CREATE DATABASE `" + cfg.Database + "`",
		ExtraStatements: ddls,
	})
	if err != nil {
		return err
	}
	_, err = op.Wait(ctx)
	return err
}

func createTestJobsData() []engine.Job {
	jobs := make([]engine.Job, 0)
	j1 := engine.NewJob("n1", "q1", []byte("hello_j1"), 120, 30, 1, "1", "")
	j2 := engine.NewJob("n1", "q2", []byte("hello_j2"), 120, 60, 1, "2", "")
	j3 := engine.NewJob("n1", "q1", []byte("hello_j3"), 120, 90, 1, "3", "")
	jobs = append(jobs, j1, j2, j3)
	return jobs
}

func createTestReqData() []*model.DBJobReq {
	req := make([]*model.DBJobReq, 0)
	r1 := &model.DBJobReq{
		PoolName:  poolName,
		Namespace: "n1",
		Queue:     "q1",
		ReadyTime: 0,
		Count:     10,
	}
	r2 := &model.DBJobReq{
		PoolName:  poolName,
		Namespace: "n1",
		Queue:     "q2",
		ReadyTime: 0,
		Count:     10,
	}
	req = append(req, r1, r2)
	return req
}

func createTestReqData2() *model.DBJobReq {
	req := &model.DBJobReq{
		PoolName:  poolName,
		ReadyTime: time.Now().Unix() + 80,
		Count:     10,
	}
	return req
}
