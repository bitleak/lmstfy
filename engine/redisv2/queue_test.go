package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
)

func TestQueue_Push(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second, time.Minute)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q1", R)
	job := engine.NewJob("ns-queue", "q1", []byte("hello msg 1"), 10, 0, 1)
	if err := q.PushInstantJob(job); err != nil {
		t.Fatalf("Failed to push job into queue: %s", err)
	}

	job2 := engine.NewJob("ns-queue", "q2", []byte("hello msg 1"), 10, 0, 1)
	if err := q.PushInstantJob(job2); err != engine.ErrWrongQueue {
		t.Fatalf("Expected to get wrong queue error, but got: %s", err)
	}
}

func TestQueue_Poll(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second, time.Minute)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q2", R)
	job := engine.NewJob("ns-queue", "q2", []byte("hello msg 2"), 10, 0, 1)
	go func() {
		time.Sleep(time.Second)
		q.PushInstantJob(job)
	}()
	jobID, _, err := q.Poll(2, 1)
	if err != nil || jobID == "" {
		t.Fatalf("Failed to poll job from queue: %s", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
}

func TestQueue_Peek(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second, time.Minute)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q3", R)
	job := engine.NewJob("ns-queue", "q3", []byte("hello msg 3"), 10, 0, 1)
	q.PushInstantJob(job)
	jobID, _, err := q.Peek()
	if err != nil || jobID == "" {
		t.Fatalf("Failed to peek job from queue: %s", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
}

func TestQueue_Destroy(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second, time.Minute)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q4", R)
	job := engine.NewJob("ns-queue", "q4", []byte("hello msg 4"), 10, 0, 1)
	p := NewPool(R)
	p.Add(job)
	q.PushInstantJob(job)
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

func TestQueue_Tries(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	namespace := "ns-queue"
	queue := "q5"
	q := NewQueue(namespace, queue, R)
	timer.AddTimerSet(q.timerset)
	var maxTries uint16 = 2
	job := engine.NewJob(namespace, queue, []byte("hello msg 5"), 30, 0, maxTries)
	q.PushInstantJob(job)
	pool := NewPool(R)
	pool.Add(job)
	jobID, _, err := q.Poll(2, 1)
	if err != nil || jobID == "" {
		t.Fatalf("Failed to poll job from queue: %v", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
	// timeout seconds should be greater than timer's MinMsgIdleTime inorder to get the renewed msg
	jobID, _, err = q.Poll(15, 1)
	if err != nil || jobID == "" {
		t.Fatalf("Failed to poll job from queue: %v", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
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
