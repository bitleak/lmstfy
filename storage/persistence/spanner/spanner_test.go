package spanner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/storage/persistence/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	poolName  = "test_pool"
	namespace = "test_ns"
)

func TestSpanner_Basic(t *testing.T) {
	ctx := context.Background()
	mgr, err := NewSpanner(config.SpannerEmulator)
	require.NoError(t, err)

	jobCnt := int64(10)
	jobIDs := make([]string, jobCnt)
	createJobs := make([]engine.Job, jobCnt)
	for i := int64(0); i < jobCnt; i++ {
		queue := "q1"
		if i%2 == 0 {
			queue = "q2"
		}
		createJobs[i] = engine.NewJob(namespace, queue, []byte("hello"), 10, 4, 3, "")
		jobIDs[i] = createJobs[i].ID()
	}
	require.NoError(t, mgr.BatchAddJobs(ctx, poolName, createJobs))

	validateJob := func(t *testing.T, job engine.Job) {
		assert.NotEmpty(t, job.ID())
		assert.EqualValues(t, job.Namespace(), namespace)
		assert.EqualValues(t, job.Tries(), 3)
		assert.GreaterOrEqual(t, job.Delay(), uint32(1))
		assert.LessOrEqual(t, job.Delay(), uint32(4))
		assert.GreaterOrEqual(t, job.TTL(), uint32(1))
		assert.LessOrEqual(t, job.TTL(), uint32(10))
	}

	t.Run("Batch Get Jobs By ID", func(t *testing.T) {
		jobs, err := mgr.BatchGetJobsByID(ctx, jobIDs)
		assert.Nil(t, err)
		assert.EqualValues(t, len(jobIDs), len(jobs))
		for _, job := range jobs {
			validateJob(t, job)
		}
	})

	t.Run("Get Ready Jobs", func(t *testing.T) {
		readyJobs, err := mgr.GetReadyJobs(ctx, &model.DBJobReq{
			PoolName:  poolName,
			ReadyTime: time.Now().Unix() + 10,
			Count:     jobCnt,
		})
		require.NoError(t, err)
		require.EqualValues(t, jobCnt, len(readyJobs))
		for _, job := range readyJobs {
			validateJob(t, job)
		}
	})

	t.Run("Get Queue Size", func(t *testing.T) {
		queueSizes, err := mgr.GetQueueSize(ctx, []*model.DBJobReq{
			{PoolName: poolName, Namespace: namespace, Queue: "q1", ReadyTime: time.Now().Unix() - 10, Count: jobCnt},
			{PoolName: poolName, Namespace: namespace, Queue: "q2", ReadyTime: time.Now().Unix() - 10, Count: jobCnt},
		})
		require.NoError(t, err)
		assert.EqualValues(t, jobCnt/2, queueSizes[fmt.Sprintf("%s/%s", namespace, "q1")])
		assert.EqualValues(t, jobCnt/2, queueSizes[fmt.Sprintf("%s/%s", namespace, "q2")])
	})

	t.Run("Del Jobs", func(t *testing.T) {
		count, err := mgr.DelJobs(context.Background(), jobIDs)
		require.NoError(t, err)
		require.EqualValues(t, jobCnt, count)
	})
}

func TestSpanner_NoExpiredJob(t *testing.T) {
	ctx := context.Background()
	mgr, err := NewSpanner(config.SpannerEmulator)
	require.NoError(t, err)

	jobCnt := int64(10)
	jobIDs := make([]string, jobCnt)
	createJobs := make([]engine.Job, jobCnt)
	for i := int64(0); i < jobCnt; i++ {
		queue := "q3"
		createJobs[i] = engine.NewJob(namespace, queue, []byte("hello"), 0, 4, 3, "")
		jobIDs[i] = createJobs[i].ID()
	}
	require.NoError(t, mgr.BatchAddJobs(ctx, poolName, createJobs))

	jobs, err := mgr.BatchGetJobsByID(ctx, jobIDs)
	assert.Nil(t, err)
	assert.EqualValues(t, len(jobIDs), len(jobs))
	for _, job := range jobs {
		assert.EqualValues(t, job.TTL(), 0)
	}
}
