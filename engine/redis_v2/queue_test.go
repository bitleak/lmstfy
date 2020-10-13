package redis_v2

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/uuid"
)

func TestQueue_Push(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-queue", "q1", R, timer)
	job := engine.NewJob("ns-queue", "q1", []byte("hello msg 1"), 10, 0, 1, 0)
	if err := q.Push(job, 5); err != nil {
		t.Fatalf("Failed to push job into queue: %s", err)
	}

	job2 := engine.NewJob("ns-queue", "q2", []byte("hello msg 1"), 10, 0, 1, 0)
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
	job := engine.NewJob("ns-queue", "q2", []byte("hello msg 2"), 10, 0, 1, 0)
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
	job := engine.NewJob("ns-queue", "q3", []byte("hello msg 3"), 10, 0, 1, 0)
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
	job := engine.NewJob("ns-queue", "q4", []byte("hello msg 4"), 10, 0, 1, 0)
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
	priority := uint8(2)
	job := engine.NewJob(namespace, queue, []byte("hello msg 5"), 30, 0, maxTries, priority)
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
	if job.Priority() != priority {
		t.Fatalf("Mismatched job priority, %d was expected but got %d", priority, job.Priority())
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

func TestPriorQueue_PollFromTimer(t *testing.T) {
	namespace := "ns-queue"
	timer, err := NewTimer("timer_set_q", R, 200*time.Millisecond)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	pool := NewPool(R)
	q := NewQueue(namespace, "q6", R, timer)
	for i := 1; i <= 10; i++ {
		priority := uint8(i)
		job := engine.NewJob(namespace, "q6", []byte("hello msg 2"), 10, 0, 1, priority)
		timer.Add(namespace, "q6", job.ID(), job.Delay(), job.Tries())
		pool.Add(job)
	}
	// make sure that all tasks were pump out by timer
	time.Sleep(time.Second)
	members, err := R.Conn.ZRangeWithScores(q.Name(), 0, -1).Result()
	if err != nil {
		t.Fatal("Failed to zrange the ready queue")
	}
	if len(members) != 10 {
		t.Fatalf("Ready queue size 10 was expected but got %d", len(members))
	}
	for i, member := range members { // this case was used to make sure that score in ready queue was right
		if i+1 != int(member.Score) {
			t.Fatalf("Invalid ready queue priority, %d was expected but got %d", i+1, int(member.Score))
		}
	}
	for i := 10; i > 0; i-- {
		jobID, _, err := q.Poll(2, 1)
		if err != nil {
			t.Fatalf("Failed to poll job from queue: %s", err.Error())
		}
		if jobID == "" {
			t.Fatalf("Got nil job")
		}
		priority, _ := uuid.ExtractPriorityFromUniqueID(jobID)
		if priority != uint8(i) {
			t.Fatalf("Mismatched job priority, %d was expected but got %d", uint8(i), priority)
		}
	}
}

func TestPriorQueue_PollWithPriority(t *testing.T) {
	timer, err := NewTimer("timer_set_q", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	namespace := "ns-queue"
	q := NewQueue(namespace, "q7", R, timer)
	jobIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		job := engine.NewJob(namespace, "q7", []byte("hello msg 7"), 10, 0, 1, uint8(i))
		q.Push(job, job.Tries())
		jobIDs[i] = job.ID()
	}
	members, err := R.Conn.ZRangeWithScores(q.Name(), 0, -1).Result()
	if err != nil {
		t.Fatal("Failed to zrange the ready queue")
	}
	for i, member := range members { // this case was used to make sure that score in ready queue was right
		if i != int(member.Score) {
			t.Fatalf("Invalid ready queue priority, %d was expected but got %d", i, int(member.Score))
		}
	}
	for i := 0; i < 10; i++ {
		jobID, _, err := q.Poll(2, 1)
		if err != nil {
			t.Fatalf("Failed to poll job from queue: %s", err.Error())
		}
		if jobID == "" {
			t.Fatalf("Got nil job")
		}
		if jobIDs[9-i] != jobID {
			t.Fatal("Mismatched job")
		}
		gotPriority, _ := uuid.ExtractPriorityFromUniqueID(jobID)
		if gotPriority != uint8(9-i) {
			t.Fatal("Mismatched job priority")
		}
	}
}

// TODO: add test case for the same priority
