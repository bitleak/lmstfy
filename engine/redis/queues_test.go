package redis

import (
	"github.com/bitleak/lmstfy/engine"
	"testing"
)

func TestQueues_ConsumeMulti(t *testing.T) {
	meta := engine.QueueMeta{
		Namespace: "ns-engine",
		Queue:     "qs1",
	}
	body := []byte("hello msg 4")
	job := engine.NewJob(meta, body, 10, 3, 1)
	q, _ := E.Queue(meta)
	jobID, err := q.Publish(job)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	meta1 := engine.QueueMeta{
		Namespace: "ns-engine",
		Queue:     "qs2",
	}
	q, _ = E.Queue(meta)
	job = engine.NewJob(meta1, body, 10, 3, 1)
	jobID2, err := q.Publish(job)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}

	qs, _ := E.Queues([]engine.QueueMeta{meta, meta1})
	job2, err := qs.Consume(5, 5)
	if err != nil {
		t.Fatalf("Failed to consume from multiple queues: %s", err)
	}
	if job2.Tries() != 0 {
		t.Fatalf("job tries = 0 was expected, but got %d", job2.Tries())
	}
	if job2.Queue() != "q5" || job2.ID() != jobID2 { // q5's job should be fired first
		t.Error("Mismatched job data")
	}

	job1, err := qs.Consume(5, 5)
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
