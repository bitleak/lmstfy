package redis

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/bitleak/lmstfy/engine"
)

func TestQueue_Publish(t *testing.T) {
	meta := engine.QueueMeta{
		Namespace: "ns-engine",
		Queue:     "q1",
	}
	body := []byte("hello msg 1")
	q, _ := E.Queue(meta)
	job := engine.NewJob(meta, body, 10, 2, 1)
	jobID, err := q.Publish(job)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}

	// Publish no-delay job
	job = engine.NewJob(meta, body, 10, 0, 1)
	jobID, err = q.Publish(job)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
}

func TestQueue_Push(t *testing.T) {
	meta := engine.QueueMeta{
		Namespace: "ns-engine",
		Queue:     "q1",
	}
	q, _ := E.Queue(meta)
	job := engine.NewJob(meta, []byte("hello msg 1"), 10, 0, 1)
	if err := q.(Queue).push(job); err != nil {
		t.Fatalf("Failed to push job into queue: %s", err)
	}

	job2 := engine.NewJob(meta, []byte("hello msg 1"), 10, 0, 1)
	if err := q.(Queue).push(job2); err != engine.ErrWrongQueue {
		t.Fatalf("Expected to get wrong queue error, but got: %s", err)
	}
}

func TestQueue_Consume(t *testing.T) {
	meta := engine.QueueMeta{
		Namespace: "ns-engine",
		Queue:     "q2",
	}
	body := []byte("hello msg 2")
	job := engine.NewJob(meta, body, 10, 2, 1)
	q, _ := E.Queue(meta)
	jobID, err := q.Publish(job)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}

	job, err = q.Consume(3, 3)
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
	job = engine.NewJob(meta, body, 10, 0, 1)
	jobID, err = q.Publish(job)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	job, err = q.Consume(3, 0)
	if err != nil {
		t.Fatalf("Failed to consume: %s", err)
	}
	if !bytes.Equal(body, job.Body()) || jobID != job.ID() {
		t.Fatalf("Mistmatched job data")
	}
}

// Consume the first one from multi publish
func TestQueue_Consume2(t *testing.T) {
	meta := engine.QueueMeta{
		Namespace: "ns-engine",
		Queue:     "q3",
	}
	body := []byte("hello msg 3")
	job := engine.NewJob(meta, []byte("delay msg"), 10, 5, 1)
	q, _ := E.Queue(meta)
	_, err := q.Publish(job)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	job = engine.NewJob(meta, body, 10, 2, 1)
	jobID, err := q.Publish(job)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	job, err = q.Consume(3, 3)
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

func TestQueue_BatchConsume(t *testing.T) {
	meta := engine.QueueMeta{
		Namespace: "ns-engine",
		Queue:     "q4",
	}
	body := []byte("hello msg 7")
	job := engine.NewJob(meta, body, 10, 3, 1)
	q, _ := E.Queue(meta)
	jobID, err := q.Publish(job)
	t.Log(jobID)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	jobs, err := q.BatchConsume(2, 5, 2)
	if err != nil {
		t.Fatalf("Failed to Batch consume: %s", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("Wrong job consumed")
	}

	jobs, err = q.BatchConsume(2, 3, 2)
	if err != nil {
		t.Fatalf("Failed to Batch consume: %s", err)
	}
	if len(jobs) != 1 || !bytes.Equal(body, jobs[0].Body()) || jobID != jobs[0].ID() {
		t.Fatalf("Mistmatched job data")
	}

	// Consume some jobs
	jobIDMap := map[string]bool{}
	for i := 0; i < 4; i++ {
		job := engine.NewJob(meta, body, 10, 0, 1)
		jobID, err := q.Publish(job)
		t.Log(jobID)
		if err != nil {
			t.Fatalf("Failed to publish: %s", err)
		}
		jobIDMap[jobID] = true
	}

	// First time batch consume three jobs
	jobs, err = q.BatchConsume(3, 3, 3)
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
	jobs, err = q.BatchConsume(3, 3, 3)
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
	jobs, err = q.BatchConsume(3, 3, 3)
	if err != nil {
		t.Fatalf("Failed to consume: %s", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("Mistmatched jobs count")
	}
}

func TestQueue_Poll(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q2", R, timer)
	job := engine.NewJob("ns-queue", "q2", []byte("hello msg 2"), 10, 0, 1)
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
	job := engine.NewJob("ns-queue", "q3", []byte("hello msg 3"), 10, 0, 1)
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
	job := engine.NewJob(namespace, queue, []byte("hello msg 5"), 30, 0, maxTries)
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
	job := engine.NewJob(namespace, queueName, []byte(msg), 30, 0, 2)
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
	job = engine.NewJob(namespace, queueName, []byte(msg), 30, 0, 2)
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
