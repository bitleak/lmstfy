package redis

import (
	"testing"
	"time"

	"github.com/meitu/lmstfy/engine"
	"github.com/sirupsen/logrus"
)

func TestTimer_Add(t *testing.T) {
	timer := NewTimer("timer_set_1", R, time.Second)
	job := engine.NewJob("ns-timer", "q1", []byte("hello msg 1"), 10, 0, 1)
	err := timer.Add(job.Namespace(), job.Queue(), job.ID(), 10, 1)
	if err != nil {
		t.Errorf("Failed to add job to timer: %s", err)
	}
}

func TestTimer_Tick(t *testing.T) {
	timer := NewTimer("timer_set_2", R, time.Second)
	defer timer.Shutdown()
	job := engine.NewJob("ns-timer", "q2", []byte("hello msg 2"), 5, 0, 1)
	pool := NewPool(R)
	pool.Add(job)
	timer.Add(job.Namespace(), job.Queue(), job.ID(), 3, 1)
	wait := make(chan struct{})
	go func() {
		defer func() {
			wait <- struct{}{}
		}()
		val, err := R.Conn.BRPop(5*time.Second, join(QueuePrefix, "ns-timer", "q2")).Result()
		if err != nil || len(val) == 0 {
			t.Fatal("Failed to pop the job from target queue")
		}
		tries, jobID, err := structUnpack(val[1])
		if err != nil {
			t.Fatalf("Failed to decode the job pop from queue")
		}
		if tries != 1 || jobID != job.ID() {
			t.Fatal("Job data mismatched")
		}
	}()
	<-wait
}

func BenchmarkTimer(b *testing.B) {
	// Disable logging temporarily
	logger.SetLevel(logrus.ErrorLevel)
	defer logger.SetLevel(logrus.DebugLevel)

	t := NewTimer("timer_set_3", R, time.Second)
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
			R.Conn.BRPop(5*time.Second, key)
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
	timer := NewTimer("timer_set_4", R, time.Second)
	timer.Shutdown()
	for i := 0; i < 10000; i++ {
		job := engine.NewJob("ns-timer", "q4", []byte("hello msg 1"), 100, 0, 1)
		pool.Add(job)
		timer.Add(job.Namespace(), job.Queue(), job.ID(), 1, 1)
	}

	b.StartTimer()
	timer.pump(time.Now().Unix() + 1)
}
