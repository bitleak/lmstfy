package redis_v2

import (
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/bitleak/lmstfy/engine"
)

func TestQueue_Push(t *testing.T) {
	q := NewQueue("ns-queue", "q1", R)
	job := engine.NewJob("ns-queue", "q1", []byte("hello msg 1"), 10, 0, 1)
	if err := q.Push(job); err != nil {
		t.Fatalf("Failed to push job into queue: %s", err)
	}

	job2 := engine.NewJob("ns-queue", "q2", []byte("hello msg 1"), 10, 0, 1)
	if err := q.Push(job2); err != engine.ErrWrongQueue {
		t.Fatalf("Expected to get wrong queue error, but got: %s", err)
	}
}

func TestQueue_Poll(t *testing.T) {
	q := NewQueue("ns-queue", "q2", R)
	job := engine.NewJob("ns-queue", "q2", []byte("hello msg 2"), 10, 0, 1)
	go func() {
		time.Sleep(time.Second)
		q.Push(job)
	}()
	jobID, err := q.Poll(2)
	if err != nil || jobID == "" {
		t.Fatalf("Failed to poll job from queue: %s", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
}

func TestQueue_Peek(t *testing.T) {
	q := NewQueue("ns-queue", "q3", R)
	job := engine.NewJob("ns-queue", "q3", []byte("hello msg 3"), 10, 0, 1)
	q.Push(job)
	jobID, err := q.Peek()
	if err != nil || jobID == "" {
		t.Fatalf("Failed to peek job from queue: %s", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
}

func TestQueue_Destroy(t *testing.T) {
	q := NewQueue("ns-queue", "q4", R)
	job := engine.NewJob("ns-queue", "q4", []byte("hello msg 4"), 10, 0, 1)
	q.Push(job)
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

func TestPopMultiQueues(t *testing.T) {
	namespace := "ns-queueName"
	queues := make([]queue, 3)
	queueNames := make([]string, 3)
	for i, queueName := range []string{"q6", "q7", "q8"} {
		queues[i] = queue{namespace: namespace, queue: queueName}
		queueNames[i] = queues[i].ReadyQueueString()
	}
	gotQueueName, gotVal, err := popMultiQueues(R, queueNames)
	if err != redis.Nil {
		t.Fatalf("redis nil err was expected, but got %s", err.Error())
	}
	if gotQueueName != "" || gotVal != "" || err != redis.Nil {
		t.Fatal("queueName name and value should be empty")
	}

	queueName := "q7"
	q := NewQueue(namespace, queueName, R)
	msg := "hello msg 7"
	job := engine.NewJob(namespace, queueName, []byte(msg), 30, 0, 2)
	q.Push(job)
	gotQueueName, gotVal, err = popMultiQueues(R, queueNames)
	if err != nil {
		t.Fatalf("redis nil err was expected, but got %s", err.Error())
	}
	if gotQueueName != q.Name() {
		t.Fatalf("invalid queueName name, %s was expected but got %s", q.Name(), gotQueueName)
	}

	// single queue condition
	queueName = "q8"
	job = engine.NewJob(namespace, queueName, []byte(msg), 30, 0, 2)
	q = NewQueue(namespace, queueName, R)
	q.Push(job)
	gotQueueName, gotVal, err = popMultiQueues(R, []string{queueNames[2]})
	if err != nil {
		t.Fatalf("redis nil err was expected, but got %s", err.Error())
	}
	if gotQueueName != q.Name() {
		t.Fatalf("invalid queueName name, %s was expected but got %s", q.Name(), gotQueueName)
	}
}
