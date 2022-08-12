package redis_v2

import (
	"fmt"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
	"github.com/sirupsen/logrus"
)

func TestTimer_Add(t *testing.T) {
	timer, err := NewTimer("timer_set_1", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	job := engine.NewJob("ns-timer", "q1", []byte("hello msg 1"), 10, 0, 1)
	if err = timer.Add(job.Namespace(), job.Queue(), job.ID(), 10, 1); err != nil {
		t.Errorf("Failed to add job to timer: %s", err)
	}
}

func TestTimer_Tick(t *testing.T) {
	timer, err := NewTimer("timer_set_2", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	job := engine.NewJob("ns-timer", "q2", []byte("hello msg 2"), 5, 0, 1)
	pool := NewPool(R)
	pool.Add(job)
	timer.Add(job.Namespace(), job.Queue(), job.ID(), 3, 1)
	errChan := make(chan error, 1)
	go func() {
		var err error = nil
		defer func() {
			// BRPop could panic
			if r := recover(); r != nil {
				err = fmt.Errorf("recover with panic %v", r)
			}
			errChan <- err
		}()
		val, err := R.Conn.BRPop(dummyCtx, 5*time.Second, join(QueuePrefix, "ns-timer", "q2")).Result()
		if err != nil || len(val) == 0 {
			err = fmt.Errorf("Failed to pop the job from target queue")
			return
		}
		tries, jobID, err := structUnpack(val[1])
		if err != nil {
			err = fmt.Errorf("Failed to decode the job pop from queue")
			return
		}
		if tries != 1 || jobID != job.ID() {
			err = fmt.Errorf("Job data mismatched")
			return
		}
	}()
	err = <-errChan
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkTimer(b *testing.B) {
	// Disable logging temporarily
	logger.SetLevel(logrus.ErrorLevel)
	defer logger.SetLevel(logrus.DebugLevel)

	t, err := NewTimer("timer_set_3", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer t.Shutdown()
	b.Run("Add", benchmarkTimer_Add(t))

	b.Run("Pop", benchmarkTimer_Pop(t))
}

func benchmarkTimer_Add(timer *Timer) func(b *testing.B) {
	pool := NewPool(R)
	return func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			job := engine.NewJob("ns-timer", "q3", []byte("hello msg 1"), 100, 0, 1)
			pool.Add(job)
			timer.Add(job.Namespace(), job.Queue(), job.ID(), 1, 1)
		}
	}
}

func benchmarkTimer_Pop(timer *Timer) func(b *testing.B) {
	return func(b *testing.B) {
		key := join(QueuePrefix, "ns-timer", "q3")
		b.StopTimer()
		pool := NewPool(R)
		for i := 0; i < b.N; i++ {
			job := engine.NewJob("ns-timer", "q3", []byte("hello msg 1"), 100, 0, 1)
			pool.Add(job)
			timer.Add(job.Namespace(), job.Queue(), job.ID(), 1, 1)
		}
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			R.Conn.BRPop(dummyCtx, 5*time.Second, key)
		}
	}
}

// How long did it take to fire 10000 due jobs
func BenchmarkTimer_Pump(b *testing.B) {
	// Disable logging temporarily
	logger.SetLevel(logrus.ErrorLevel)
	defer logger.SetLevel(logrus.DebugLevel)

	b.StopTimer()

	pool := NewPool(R)
	timer, err := NewTimer("timer_set_4", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	timer.Shutdown()
	for i := 0; i < 10000; i++ {
		job := engine.NewJob("ns-timer", "q4", []byte("hello msg 1"), 100, 0, 1)
		pool.Add(job)
		timer.Add(job.Namespace(), job.Queue(), job.ID(), 1, 1)
	}

	b.StartTimer()
	timer.pump(time.Now().Unix() + 1)
}
