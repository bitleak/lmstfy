package redis_v2

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	redisMaxMemory = "10000000"
	dummyPoolConf  = &config.RedisConf{}
	attributes     = map[string]string{"flag": "1", "label": "abc"}
)

func TestEngine_Publish(t *testing.T) {
	e, err := NewEngine(R.Name, dummyPoolConf, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 1")
	j := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q1",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      2,
		Tries:      1,
		Attributes: nil,
	})
	jobID, err := e.Publish(j)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}

	// Publish no-delay job
	j = engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q1",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      0,
		Tries:      1,
		Attributes: nil,
	})
	jobID, err = e.Publish(j)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}

	// Publish no-delay job with attributes
	j = engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q1",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      0,
		Tries:      1,
		Attributes: attributes,
	})
	jobID, err = e.Publish(j)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
}

func TestEngine_Publish_SecondaryStorage(t *testing.T) {
	manager, err := storage.NewManger(testConfig.Config)
	require.Nil(t, err)

	e, err := NewEngine(R.Name, &config.RedisConf{
		EnableSecondaryStorage:           true,
		SecondaryStorageThresholdSeconds: 0,
	}, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()

	err = R.Conn.ConfigSet(dummyCtx, "maxmemory", redisMaxMemory).Err()
	assert.Nil(t, err)

	defer manager.Shutdown()
	manager.AddPool(R.Name, e, 0)
	// Publish long-delay job
	body := []byte("hello msg long delay job")
	j := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "qs",
		ID:         "",
		Body:       body,
		TTL:        120,
		Delay:      1,
		Tries:      1,
		Attributes: attributes,
	})
	jobID, err := e.Publish(j)
	assert.Nil(t, err)

	job, err := e.Consume("ns-engine", []string{"qs"}, 3, 10)
	assert.Nil(t, err)
	assert.EqualValues(t, jobID, job.ID())
	assert.EqualValues(t, body, job.Body())
	assert.EqualValues(t, body, job.Body())
	assert.NotNil(t, job.Attributes())
	assert.EqualValues(t, "1", job.Attributes()["flag"])
	assert.EqualValues(t, "abc", job.Attributes()["label"])
}

func TestEngine_Consume(t *testing.T) {
	e, err := NewEngine(R.Name, dummyPoolConf, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 2")
	//j := engine.NewJob("ns-engine", "q2", body, 10, 2, 1, "", "")
	j := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q2",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      2,
		Tries:      1,
		Attributes: nil,
	})
	jobID, err := e.Publish(j)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	job, err := e.Consume("ns-engine", []string{"q2"}, 3, 3)
	if err != nil {
		t.Fatalf("Failed to consume: %s", err)
	}
	if job.Tries() != 0 {
		t.Fatalf("job tries = 0 was expected, but got %d", job.Tries())
	}
	if !bytes.Equal(body, job.Body()) || jobID != job.ID() {
		t.Fatalf("Mistmatched job data")
	}

	// Consume job that's published in no-delay way
	j = engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q2",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      0,
		Tries:      1,
		Attributes: nil,
	})
	jobID, err = e.Publish(j)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	job, err = e.Consume("ns-engine", []string{"q2"}, 3, 0)
	if err != nil {
		t.Fatalf("Failed to consume: %s", err)
	}
	if !bytes.Equal(body, job.Body()) || jobID != job.ID() {
		t.Fatalf("Mistmatched job data")
	}

	// Consume job with attributes
	j = engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q2",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      0,
		Tries:      1,
		Attributes: attributes,
	})
	jobID, err = e.Publish(j)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	job, err = e.Consume("ns-engine", []string{"q2"}, 3, 0)
	if err != nil {
		t.Fatalf("Failed to consume: %s", err)
	}
	if !bytes.Equal(body, job.Body()) || jobID != job.ID() {
		t.Fatalf("Mistmatched job data")
	}
	assert.NotNil(t, job.Attributes())
	assert.EqualValues(t, "1", job.Attributes()["flag"])
	assert.EqualValues(t, "abc", job.Attributes()["label"])
}

// Consume the first one from multi publish
func TestEngine_Consume2(t *testing.T) {
	e, err := NewEngine(R.Name, dummyPoolConf, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 3")
	j := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q3",
		ID:         "",
		Body:       []byte("delay msg"),
		TTL:        10,
		Delay:      5,
		Tries:      1,
		Attributes: nil,
	})

	_, err = e.Publish(j)
	j = engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q3",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      2,
		Tries:      1,
		Attributes: nil,
	})
	jobID, err := e.Publish(j)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	job, err := e.Consume("ns-engine", []string{"q3"}, 3, 3)
	if err != nil {
		t.Fatalf("Failed to consume: %s", err)
	}
	if job.Tries() != 0 {
		t.Fatalf("job tries = 0 was expected, but got %d", job.Tries())
	}
	if !bytes.Equal(body, job.Body()) || jobID != job.ID() {
		t.Fatalf("Mistmatched job data")
	}
}

func TestEngine_ConsumeMulti(t *testing.T) {
	e, err := NewEngine(R.Name, dummyPoolConf, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 4")
	j := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q4",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      3,
		Tries:      1,
		Attributes: nil,
	})
	jobID, err := e.Publish(j)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	j = engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q5",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      1,
		Tries:      1,
		Attributes: nil,
	})
	jobID2, err := e.Publish(j)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}

	job2, err := e.Consume("ns-engine", []string{"q4", "q5"}, 5, 5)
	if err != nil {
		t.Fatalf("Failed to consume from multiple queues: %s", err)
	}
	if job2.Tries() != 0 {
		t.Fatalf("job tries = 0 was expected, but got %d", job2.Tries())
	}
	if job2.Queue() != "q5" || job2.ID() != jobID2 { // q5's job should be fired first
		t.Error("Mismatched job data")
	}

	job1, err := e.Consume("ns-engine", []string{"q4", "q5"}, 5, 5)
	if err != nil {
		t.Fatalf("Failed to consume from multiple queues: %s", err)
	}
	if job1.Tries() != 0 {
		t.Fatalf("job tries = 0 was expected, but got %d", job1.Tries())
	}
	if job1.Queue() != "q4" || job1.ID() != jobID { // q4's job should be fired next
		t.Fatalf("Failed to consume from multiple queues: %s", err)
	}
}

func TestEngine_Peek(t *testing.T) {
	e, err := NewEngine(R.Name, dummyPoolConf, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 6")
	j := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q6",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      0,
		Tries:      1,
		Attributes: nil,
	})
	jobID, err := e.Publish(j)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	job, err := e.Peek("ns-engine", "q6", "")
	if err != nil {
		t.Fatalf("Failed to peek: %s", err)
	}
	if job.ID() != jobID || !bytes.Equal(job.Body(), body) {
		t.Fatal("Mismatched job")
	}

	_, err = e.Consume("ns-engine", []string{"q6"}, 5, 0)
	if err != nil {
		t.Fatalf("Failed to consume previous queue job: %s", err)
	}

	// test peek job with attributes
	j = engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q6",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      0,
		Tries:      1,
		Attributes: attributes,
	})
	jobID, err = e.Publish(j)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	job, err = e.Peek("ns-engine", "q6", "")
	if err != nil {
		t.Fatalf("Failed to peek: %s", err)
	}
	if job.ID() != jobID || !bytes.Equal(job.Body(), body) {
		t.Fatal("Mismatched job")
	}
	assert.NotNil(t, job.Attributes())
	assert.EqualValues(t, "1", job.Attributes()["flag"])
	assert.EqualValues(t, "abc", job.Attributes()["label"])
}

func TestEngine_Peek_SecondaryStorage(t *testing.T) {
	e, err := NewEngine(R.Name, &config.RedisConf{
		EnableSecondaryStorage:           true,
		SecondaryStorageThresholdSeconds: 10,
	}, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()

	manager, err := storage.NewManger(testConfig.Config)
	defer manager.Shutdown()
	require.Nil(t, err)
	manager.AddPool(R.Name, e, 30)

	// Publish long-delay job
	body := []byte("engine peek long delay job")
	j := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "qst",
		ID:         "",
		Body:       body,
		TTL:        120,
		Delay:      45,
		Tries:      1,
		Attributes: attributes,
	})

	jobID, err := e.Publish(j)
	t.Log(jobID)
	assert.Nil(t, err)
	job, err := e.Peek("ns-engine", "qst", "")
	assert.Nil(t, job)
	assert.EqualValues(t, engine.ErrEmptyQueue, err)
	job, err = e.Peek("ns-engine", "qst", jobID)
	assert.Nil(t, err)
	assert.EqualValues(t, jobID, job.ID())
	assert.EqualValues(t, body, job.Body())
	assert.NotNil(t, job.Attributes())
	assert.EqualValues(t, "1", job.Attributes()["flag"])
	assert.EqualValues(t, "abc", job.Attributes()["label"])
}

func TestEngine_BatchConsume(t *testing.T) {
	e, err := NewEngine(R.Name, dummyPoolConf, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 7")
	j := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q7",
		ID:         "",
		Body:       body,
		TTL:        10,
		Delay:      3,
		Tries:      1,
		Attributes: nil,
	})
	jobID, err := e.Publish(j)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	queues := []string{"q7"}
	jobs, err := e.BatchConsume("ns-engine", queues, 2, 5, 2)
	if err != nil {
		t.Fatalf("Failed to Batch consume: %s", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("Wrong job consumed")
	}

	jobs, err = e.BatchConsume("ns-engine", queues, 2, 3, 2)
	if err != nil {
		t.Fatalf("Failed to Batch consume: %s", err)
	}
	if len(jobs) != 1 || !bytes.Equal(body, jobs[0].Body()) || jobID != jobs[0].ID() {
		t.Fatalf("Mistmatched job data")
	}

	// Consume some jobs
	jobIDMap := map[string]bool{}
	for i := 0; i < 4; i++ {
		j := engine.NewJobFromReq(&engine.CreateJobReq{
			Namespace:  "ns-engine",
			Queue:      "q7",
			ID:         "",
			Body:       body,
			TTL:        10,
			Delay:      0,
			Tries:      1,
			Attributes: nil,
		})
		jobID, err := e.Publish(j)
		t.Log(jobID)
		if err != nil {
			t.Fatalf("Failed to publish: %s", err)
		}
		jobIDMap[jobID] = true
	}

	// First time batch consume three jobs
	jobs, err = e.BatchConsume("ns-engine", queues, 3, 3, 3)
	if err != nil {
		t.Fatalf("Failed to consume: %s", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("Mistmatched jobs count")
	}
	for _, job := range jobs {
		if !bytes.Equal(body, job.Body()) || !jobIDMap[job.ID()] {
			t.Fatalf("Mistmatched job data")
		}
	}

	// Second time batch consume can only get a single job
	jobs, err = e.BatchConsume("ns-engine", queues, 3, 3, 3)
	if err != nil {
		t.Fatalf("Failed to consume: %s", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("Mistmatched jobs count")
	}
	if !bytes.Equal(body, jobs[0].Body()) || !jobIDMap[jobs[0].ID()] {
		t.Fatalf("Mistmatched job data")
	}

	// Third time batch consume will be blocked by 3s
	jobs, err = e.BatchConsume("ns-engine", queues, 3, 3, 3)
	if err != nil {
		t.Fatalf("Failed to consume: %s", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("Mistmatched jobs count")
	}
}

func TestEngine_PublishWithJobID(t *testing.T) {
	e, err := NewEngine(R.Name, dummyPoolConf, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 1")
	j := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-engine",
		Queue:      "q8",
		ID:         "jobID1",
		Body:       body,
		TTL:        10,
		Delay:      0,
		Tries:      1,
		Attributes: nil,
	})
	jobID, err := e.Publish(j)
	t.Log(jobID)
	assert.Nil(t, err)

	// Make sure the engine received the job
	job, err := e.Consume("ns-engine", []string{"q8"}, 3, 0)
	assert.EqualValues(t, jobID, job.ID())
}
