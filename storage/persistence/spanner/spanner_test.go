package spanner

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	db       = "projects/test-project/instances/test-instance/databases/test-db"
	dummyCtx = context.TODO()
)

func init() {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		panic(fmt.Sprintf("failed to find $SPANNER_EMULATOR_HOST value"))
	}
	err := CreateInstance(dummyCtx, db)
	if err != nil {
		panic(fmt.Sprintf("create instance error: %v", err))
	}
	err = CreateDatabase(dummyCtx, db)
	if err != nil {
		panic(fmt.Sprintf("create db error: %v", err))
	}
}

func TestCreateSpannerClient(t *testing.T) {
	_, err := CreateSpannerClient(cfg)
	assert.Nil(t, err)
}

func TestSpannerDataMgr_BatchAddDelJobs(t *testing.T) {
	mgr, err := NewSpanner(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to create spanner client with error: %s", err))
	}
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
	mgr, err := NewSpanner(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to create spanner client with error: %s", err))
	}
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
	mgr, err := NewSpanner(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to create spanner client with error: %s", err))
	}
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

func TestSpannerDataMgr_GetReadyJobs(t *testing.T) {
	mgr, err := NewSpanner(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to create spanner client with error: %s", err))
	}
	jobs := createTestJobsData()
	mgr.BatchAddJobs(ctx, jobs)
	req := createTestReqData2()
	jobs, err = mgr.GetReadyJobs(ctx, req)
	if err != nil {
		panic(fmt.Sprintf("GetReadyJobs failed with error: %s", err))
	}
	assert.EqualValues(t, 2, len(jobs))
	mgr.DelJobs(ctx, jobIDs)
}

func TestSpannerDataMgr_BatchGetJobsByID(t *testing.T) {
	mgr, err := NewSpanner(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to create spanner client with error: %s", err))
	}
	jobs := createTestJobsData()
	mgr.BatchAddJobs(ctx, jobs)
	IDs := []string{"1", "2", "3"}
	jobs, err = mgr.BatchGetJobsByID(ctx, IDs)
	assert.Nil(t, err)
	assert.EqualValues(t, 3, len(jobs))
	mgr.DelJobs(ctx, jobIDs)
}
