package redis_v2

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestQueue_Push(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second, 600*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q1", R, timer)
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
	timer, err := NewTimer("timer_set_q", R, time.Second, 600*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q2", R, timer)
	job := engine.NewJob("ns-queue", "q2", []byte("hello msg 2"), 10, 0, 1)
	go func() {
		time.Sleep(time.Second)
		q.Push(job)
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
	timer, err := NewTimer("timer_set_q", R, time.Second, 600*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q3", R, timer)
	job := engine.NewJob("ns-queue", "q3", []byte("hello msg 3"), 10, 0, 2)
	q.Push(job)
	jobID, tries, err := q.Peek()
	if err != nil || jobID == "" || tries != 2 {
		t.Fatalf("Failed to peek job from queue: %s", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
}

func TestQueue_Destroy(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second, 600*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q4", R, timer)
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

func TestQueue_Tries(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second, 600*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	namespace := "ns-queue"
	queue := "q5"
	q := NewQueue(namespace, queue, R, timer)
	var maxTries uint16 = 2
	job := engine.NewJob(namespace, queue, []byte("hello msg 5"), 30, 0, maxTries)
	q.Push(job)
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
	timer, err := NewTimer("timer_set_q", R, time.Second, 600*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()

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
	q := NewQueue(namespace, queueName, R, timer)
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
	q = NewQueue(namespace, queueName, R, timer)
	q.Push(job)
	gotQueueName, gotVal, err = popMultiQueues(R, []string{queueNames[2]})
	if err != nil {
		t.Fatalf("redis nil err was expected, but got %s", err.Error())
	}
	if gotQueueName != q.Name() {
		t.Fatalf("invalid queueName name, %s was expected but got %s", q.Name(), gotQueueName)
	}
}

func TestQueue_Backup(t *testing.T) {
	timer, err := NewTimer("timer_set_for_test_backup", R, time.Second, 600*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()

	namespace := "ns-queue"
	queue := "q9"
	q := NewQueue(namespace, queue, R, timer)
	count := 10
	for i := 0; i < count; i++ {
		delay := uint32(0)
		if i%2 == 0 {
			delay = 1
		}
		job := engine.NewJob(namespace, queue, []byte("hello msg"), 30, delay, 2)
		q.Push(job)
		pool := NewPool(R)
		pool.Add(job)
	}
	backupName := timer.BackupName()
	memberScores, err := q.redis.Conn.ZRangeWithScores(dummyCtx, backupName, 0, -1).Result()
	assert.Nil(t, err)
	now := time.Now().Unix()
	for _, memberScore := range memberScores {
		gotNamespace, gotQueue, gotJobID, err := structUnpackTimerData([]byte(memberScore.Member.(string)))
		assert.Nil(t, err)
		assert.Equal(t, namespace, gotNamespace)
		assert.Equal(t, queue, gotQueue)
		assert.Equal(t, 26, len(gotJobID))
		timestamp, tries := decodeScore(memberScore.Score)
		assert.Equal(t, uint16(2), tries)
		assert.LessOrEqual(t, timestamp, now)
	}

	for i := 0; i < 10; i++ {
		jobID, _, err := q.Poll(2, 1)
		if err != nil || jobID == "" {
			t.Fatalf("Failed to poll job from queue: %s", err)
		}
	}
	backupCount, err := q.redis.Conn.ZCard(dummyCtx, backupName).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), backupCount)
}
