package redis

import (
	"testing"
	"time"

	"github.com/meitu/lmstfy/engine"
)

func TestQueue_Push(t *testing.T) {
	timer := NewTimer("timer_set_q", R, time.Second)
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q1", R, timer)
	job := engine.NewJob("ns-queue", "q1", []byte("hello msg 1"), 10, 0, 1)
	if err := q.Push(job, 5); err != nil {
		t.Fatalf("Failed to push job into queue: %s", err)
	}

	job2 := engine.NewJob("ns-queue", "q2", []byte("hello msg 1"), 10, 0, 1)
	if err := q.Push(job2, 5); err != engine.ErrWrongQueue {
		t.Fatalf("Expected to get wrong queue error, but got: %s", err)
	}
}

func TestQueue_Poll(t *testing.T) {
	timer := NewTimer("timer_set_q", R, time.Second)
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q2", R, timer)
	job := engine.NewJob("ns-queue", "q2", []byte("hello msg 2"), 10, 0, 1)
	go func() {
		time.Sleep(time.Second)
		q.Push(job, 2)
	}()
	jobID, err := q.Poll(2, 5)
	if err != nil || jobID == "" {
		t.Fatalf("Failed to poll job from queue: %s", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
}

func TestQueue_Peek(t *testing.T) {
	timer := NewTimer("timer_set_q", R, time.Second)
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q3", R, timer)
	job := engine.NewJob("ns-queue", "q3", []byte("hello msg 3"), 10, 0, 1)
	q.Push(job, 2)
	jobID, err := q.Peek()
	if err != nil || jobID == "" {
		t.Fatalf("Failed to peek job from queue: %s", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
}

func TestQueue_Destroy(t *testing.T) {
	timer := NewTimer("timer_set_q", R, time.Second)
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q4", R, timer)
	job := engine.NewJob("ns-queue", "q4", []byte("hello msg 4"), 10, 0, 1)
	q.Push(job, 2)
	count, err := q.Destroy()
	if err != nil {
		t.Fatalf("Failed to destroy queue: %s", err)
	}
	if count != 1 {
		t.Fatalf("Mismatched deleted jobs count")
	}
	size, _ := q.Size()
	if size != 0 {
		t.Fatalf("Destroyed queue should be of size 0")
	}
}

func TestStructPacking(t *testing.T) {
	var tries uint16 = 23
	jobID := " a test ID#"
	data := structPack(tries, jobID)
	tries2, jobID2, err := structUnpack(data)
	if err != nil {
		t.Fatal("Failed to unpack")
	}
	if tries != tries2 || jobID != jobID2 {
		t.Fatal("Mismatched unpack data")
	}
}
