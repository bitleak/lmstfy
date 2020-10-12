package redis

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/uuid"
)

const (
	_namespace = "ns-prior-queue"
	_timerName = "timer_set_prior_q"
)

func TestPriorQueue_Push(t *testing.T) {
	timer, err := NewPriorQueueTimer(_timerName, R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewPriorQueue(_namespace, "q1", R, timer)
	job := engine.NewJob(_namespace, "q1", []byte("hello msg 1"), 10, 0, 1, 0)
	if err := q.Push(job, job.Tries()); err != nil {
		t.Fatalf("Failed to push job into queue: %s", err)
	}

	job2 := engine.NewJob(_namespace, "q2", []byte("hello msg 1"), 10, 0, 1, 0)
	if err := q.Push(job2, job2.Tries()); err != engine.ErrWrongQueue {
		t.Fatalf("Expected to get wrong queue error, but got: %s", err.Error())
	}
}

func TestPriorQueue_Poll(t *testing.T) {
	timer, err := NewPriorQueueTimer(_timerName, R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewPriorQueue(_namespace, "q2", R, timer)
	priority := uint8(3)
	job := engine.NewJob(_namespace, "q2", []byte("hello msg 2"), 10, 0, 1, priority)
	go func() {
		time.Sleep(time.Second)
		q.Push(job, job.Tries())
	}()
	jobID, _, err := q.Poll(2, 1)
	if err != nil {
		t.Fatalf("Failed to poll job from queue: %s", err.Error())
	}
	if jobID == "" {
		t.Fatalf("Got nil job")
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
	if job.Priority() != priority {
		t.Fatalf("Mismatched job priority, %d was expected but got %d", priority, job.Priority())
	}
}

func TestPriorQueue_Peek(t *testing.T) {
	timer, err := NewPriorQueueTimer(_timerName, R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewPriorQueue(_namespace, "q3", R, timer)
	job := engine.NewJob(_namespace, "q3", []byte("hello msg 3"), 10, 0, 1, 0)
	q.Push(job, 2)
	jobID, tries, err := q.Peek()
	if err != nil || jobID == "" || tries != 2 {
		t.Fatalf("Failed to peek job from queue: %s", err)
	}
	if job.ID() != jobID {
		t.Fatal("Mismatched job")
	}
}

func TestPriorQueue_Destroy(t *testing.T) {
	timer, err := NewPriorQueueTimer(_timerName, R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewPriorQueue(_namespace, "q4", R, timer)
	expectedCount := 8
	for i := 0; i < expectedCount; i++ {
		job := engine.NewJob(_namespace, "q4", []byte("hello msg 4"), 10, 0, 1, 0)
		q.Push(job, job.Tries())
	}
	count, err := q.Destroy()
	if err != nil {
		t.Fatalf("Failed to destroy queue: %s", err)
	}
	if int(count) != expectedCount {
		t.Fatalf("Mismatched deleted jobs count, expected %d but got %d", expectedCount, count)
	}
	size, _ := q.Size()
	if size != 0 {
		t.Fatalf("Destroyed queue should be of size 0")
	}
}

func TestPriorQueue_Tries(t *testing.T) {
	timer, err := NewPriorQueueTimer(_timerName, R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	queue := "q5"
	q := NewPriorQueue(_namespace, queue, R, timer)
	var maxTries uint16 = 2
	priority := uint8(2)
	job := engine.NewJob(_namespace, queue, []byte("hello msg 5"), 30, 0, maxTries, priority)
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

func TestPriorQueue_PollFromTimer(t *testing.T) {
	timer, err := NewPriorQueueTimer(_timerName, R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	pool := NewPool(R)
	q := NewPriorQueue(_namespace, "q6", R, timer)
	for i := 1; i <= 10; i++ {
		priority := uint8(i)
		job := engine.NewJob(_namespace, "q6", []byte("hello msg 2"), 10, 1, 1, priority)
		timer.Add(_namespace, "q6", job.ID(), job.Delay(), job.Tries())
		pool.Add(job)
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
	timer, err := NewPriorQueueTimer(_timerName, R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewPriorQueue(_namespace, "q7", R, timer)
	jobIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		job := engine.NewJob(_namespace, "q7", []byte("hello msg 7"), 10, 0, 1, uint8(i))
		q.Push(job, job.Tries())
		jobIDs[i] = job.ID()
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

func TestPriorQueue_PollWithSamePriority(t *testing.T) {
	timer, err := NewPriorQueueTimer(_timerName, R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewPriorQueue(_namespace, "q7", R, timer)
	jobIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		job := engine.NewJob(_namespace, "q7", []byte("hello msg 7"), 10, 0, 1, 3)
		q.Push(job, job.Tries())
		jobIDs[i] = job.ID()
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
		if gotPriority != 3 {
			t.Fatal("Mismatched job priority")
		}
	}
}
