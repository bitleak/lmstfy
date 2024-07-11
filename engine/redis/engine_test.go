package redis

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bitleak/lmstfy/engine"
)

func TestEngine_Publish(t *testing.T) {
	e, err := NewEngine(R.Name, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 1")
	j := engine.NewJob("ns-engine", "q0", body, nil, 10, 2, 1, "")
	jobID, err := e.Publish(j)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}

	// Publish no-delay job
	j = engine.NewJob("ns-engine", "q0", body, nil, 10, 0, 1, "")
	jobID, err = e.Publish(j)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
}

func TestEngine_Consume(t *testing.T) {
	e, err := NewEngine(R.Name, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 2")
	j := engine.NewJob("ns-engine", "q2", body, nil, 10, 2, 1, "")
	jobID, err := e.Publish(j)
	t.Log(jobID)
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
	j = engine.NewJob("ns-engine", "q2", body, nil, 10, 0, 1, "")
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
		t.Fatal("Mistmatched job data")
	}
}

// Consume the first one from multi publish
func TestEngine_Consume2(t *testing.T) {
	e, err := NewEngine(R.Name, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 3")
	j := engine.NewJob("ns-engine", "q3", []byte("delay msg"), nil, 10, 5, 1, "")
	_, err = e.Publish(j)
	j = engine.NewJob("ns-engine", "q3", body, nil, 10, 2, 1, "")
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
		t.Fatal("Mistmatched job data")
	}
}

func TestEngine_ConsumeMulti(t *testing.T) {
	e, err := NewEngine(R.Name, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 4")
	j := engine.NewJob("ns-engine", "q4", body, nil, 10, 3, 1, "")
	jobID, err := e.Publish(j)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	j2 := engine.NewJob("ns-engine", "q5", body, nil, 10, 1, 1, "")
	jobID2, err := e.Publish(j2)
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
	e, err := NewEngine(R.Name, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 6")
	j := engine.NewJob("ns-engine", "q6", body, nil, 10, 0, 1, "")
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
}

func TestEngine_BatchConsume(t *testing.T) {
	e, err := NewEngine(R.Name, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 7")
	j := engine.NewJob("ns-engine", "q7", body, nil, 10, 3, 1, "")
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
		j := engine.NewJob("ns-engine", "q7", body, nil, 10, 0, 1, "")
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
	e, err := NewEngine(R.Name, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 1")
	j := engine.NewJob("ns-engine", "q8", body, nil, 10, 0, 1, "jobID1")
	jobID, err := e.Publish(j)
	t.Log(jobID)
	assert.Nil(t, err)

	// Make sure the engine received the job
	job, err := e.Consume("ns-engine", []string{"q8"}, 3, 0)
	assert.EqualValues(t, jobID, job.ID())
}
