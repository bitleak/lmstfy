package redis

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/bitleak/lmstfy/engine"
)

func TestQueue_Push(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q1", R, timer)
	job := engine.NewJob("ns-queue", "q1", []byte("hello msg 1"), nil, 10, 0, 1, "")
	if err := q.Push(job, 5); err != nil {
		t.Fatalf("Failed to push job into queue: %s", err)
	}

	job2 := engine.NewJob("ns-queue", "q2", []byte("hello msg 1"), nil, 10, 0, 1, "")
	if err := q.Push(job2, 5); err != engine.ErrWrongQueue {
		t.Fatalf("Expected to get wrong queue error, but got: %s", err)
	}
}

func TestQueue_Poll(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q2", R, timer)
	job := engine.NewJob("ns-queue", "q2", []byte("hello msg 2"), nil, 10, 0, 1, "")
	go func() {
		time.Sleep(time.Second)
		q.Push(job, 2)
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
	timer, err := NewTimer("timer_set_q", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q3", R, timer)
	job := engine.NewJob("ns-queue", "q3", []byte("hello msg 3"), nil, 10, 0, 1, "")
	q.Push(job, 2)
	jobID, tries, err := q.Peek()
	if err != nil || jobID == "" || tries != 2 {
		t.Fatalf("Failed to peek job from queue: %s", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
}

func TestQueue_Destroy(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q4", R, timer)
	job := engine.NewJob("ns-queue", "q4", []byte("hello msg 4"), nil, 10, 0, 1, "")
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

func TestQueue_Tries(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	namespace := "ns-queue"
	queue := "q5"
	q := NewQueue(namespace, queue, R, timer)
	var maxTries uint16 = 2
	job := engine.NewJob(namespace, queue, []byte("hello msg 5"), nil, 30, 0, maxTries, "")
	q.Push(job, maxTries)
	pool := NewPool(R)
	pool.Add(job)
	jobID, tries, err := q.Poll(2, 1)
	if err != nil || jobID == "" {
		t.Fatalf("Failed to poll job from queue: %s", err)
	}
	if tries != (maxTries - 1) {
		t.Fatalf("Expected to get tries 1 , but got " + strconv.Itoa(int(tries)))
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
	jobID, tries, err = q.Poll(5, 1)
	if err != nil || jobID == "" {
		t.Fatalf("Failed to poll job from queue: %s", err)
	}
	if tries != (maxTries - 2) {
		t.Fatalf("Expected to get tries 0 , but got " + strconv.Itoa(int(tries)))
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

func TestPopMultiQueues(t *testing.T) {
	namespace := "ns-queueName"
	queues := make([]QueueName, 3)
	queueNames := make([]string, 3)
	for i, queueName := range []string{"q6", "q7", "q8"} {
		queues[i] = QueueName{Namespace: namespace, Queue: queueName}
		queueNames[i] = queues[i].String()
	}
	gotQueueName, gotVal, err := popMultiQueues(R, queueNames)
	if err != redis.Nil {
		t.Fatalf("redis nil err was expected, but got %s", err.Error())
	}
	if gotQueueName != "" || gotVal != "" || err != redis.Nil {
		t.Fatal("queueName name and value should be empty")
	}

	queueName := "q7"
	q := NewQueue(namespace, queueName, R, nil)
	msg := "hello msg 7"
	job := engine.NewJob(namespace, queueName, []byte(msg), nil, 30, 0, 2, "")
	q.Push(job, 2)
	gotQueueName, gotVal, err = popMultiQueues(R, queueNames)
	if err != nil {
		t.Fatalf("redis nil err was expected, but got %s", err.Error())
	}
	if gotQueueName != q.Name() {
		t.Fatalf("invalid queueName name, %s was expected but got %s", q.Name(), gotQueueName)
	}

	// single queue condition
	queueName = "q8"
	job = engine.NewJob(namespace, queueName, []byte(msg), nil, 30, 0, 2, "")
	q = NewQueue(namespace, queueName, R, nil)
	q.Push(job, 2)
	gotQueueName, gotVal, err = popMultiQueues(R, []string{queueNames[2]})
	if err != nil {
		t.Fatalf("redis nil err was expected, but got %s", err.Error())
	}
	if gotQueueName != q.Name() {
		t.Fatalf("invalid queueName name, %s was expected but got %s", q.Name(), gotQueueName)
	}
}
