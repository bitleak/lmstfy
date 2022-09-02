package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/storage/persistence/model"
)

var dummyCtx = context.TODO()

func TestNewManager(t *testing.T) {
	cfg := testConfig.Config
	cfg.SecondaryStorage = nil
	_, err := NewManger(cfg)
	assert.NotNil(t, err)

	cfg.SecondaryStorage = &config.SecondaryStorage{Spanner: config.SpannerEmulator}
	_, err = NewManger(cfg)
	assert.Nil(t, err)
}

func TestManager_AddJob_GetJob(t *testing.T) {
	cfg := testConfig.Config
	mgr, err := NewManger(cfg)
	assert.Nil(t, err)

	job := &model.JobData{
		PoolName:    "default",
		JobID:       "j1",
		Namespace:   "n1",
		Queue:       "qm1",
		Body:        []byte("hello_j1"),
		ExpiredTime: time.Now().Unix() + 120,
		ReadyTime:   time.Now().Unix() + 200,
		Tries:       1,
		CreatedTime: time.Now().Unix(),
	}
	err = mgr.AddJob(dummyCtx, job)
	assert.Nil(t, err)

	res, err := mgr.GetJobByID(dummyCtx, job.JobID)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, len(res))
	assert.EqualValues(t, job.JobID, res[0].JobID)
	assert.EqualValues(t, job.Body, res[0].Body)
}

func init() {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		panic(fmt.Sprintf("failed to find $SPANNER_EMULATOR_HOST value"))
	}
	err := CreateInstance(dummyCtx, config.SpannerEmulator)
	if err != nil {
		panic(fmt.Sprintf("create instance error: %v", err))
	}
	err = CreateDatabase(dummyCtx, config.SpannerEmulator)
	if err != nil {
		panic(fmt.Sprintf("create db error: %v", err))
	}
}
