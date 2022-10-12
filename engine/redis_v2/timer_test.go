package redis_v2

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bitleak/lmstfy/engine"
)

func TestTimer_Add(t *testing.T) {
	timer, err := NewTimer("timer_set_1", R, time.Second, 600*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	job := engine.NewJob("ns-timer", "q1", []byte("hello msg 1"), 10, 0, 1, "", "")
	if err = timer.Add(job.Namespace(), job.Queue(), job.ID(), 10, 1); err != nil {
		t.Errorf("Failed to add job to timer: %s", err)
	}
}

func TestTimer_Tick(t *testing.T) {
	timer, err := NewTimer("timer_set_2", R, time.Second, 600*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	job := engine.NewJob("ns-timer", "q2", []byte("hello msg 2"), 5, 0, 1, "", "")
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

func TestBackupTimer_BeforeOldestScore(t *testing.T) {
	timer, err := NewTimer("test_backup_before_oldest_score", R, time.Second, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()

	pool := NewPool(R)
	ns := "ns-test-backup"
	queueName := "q0"
	queue := NewQueue(ns, queueName, R, timer)
	count := 10
	for i := 0; i < count; i++ {
		job := engine.NewJob(ns, queueName, []byte("hello msg"+strconv.Itoa(i)), 100, 1, 3, "", "")
		pool.Add(job)
		if i%2 == 0 {
			queue.Push(job)
		} else {
			timer.Add(job.Namespace(), job.Queue(), job.ID(), job.Delay(), job.Tries())
		}
	}
	// make sure all jobs are in the ready queue and consume them without ACK,
	// so they should appear in the backup queue.
	time.Sleep(2 * time.Second)
	assert.Equal(t, int64(count), R.Conn.ZCard(dummyCtx, timer.BackupName()).Val())
	for i := 0; i < count/2; i++ {
		_, err := R.Conn.BRPop(dummyCtx, time.Second, queue.Name()).Result()
		assert.Nil(t, err)
	}
	require.Equal(t, int64(count)/2, R.Conn.LLen(dummyCtx, queue.Name()).Val())
	time.Sleep(3 * time.Second)
	// all jobs should requeue in ready queue again
	require.Equal(t, int64(count), R.Conn.LLen(dummyCtx, queue.Name()).Val())
	require.Equal(t, int64(count), R.Conn.ZCard(dummyCtx, timer.BackupName()).Val())

	for i := 0; i < count; i++ {
		val, err := R.Conn.BRPop(dummyCtx, time.Second, queue.Name()).Result()
		assert.Nil(t, err)
		tries, jobID, err := structUnpack(val[1])
		assert.Nil(t, err)
		assert.Equal(t, uint16(3), tries)
		assert.Nil(t, pool.Delete(ns, queueName, jobID))
	}
	// backup jobs should be disappeared after jobs were ACKed
	time.Sleep(2 * time.Second)
	require.Equal(t, int64(0), R.Conn.LLen(dummyCtx, queue.Name()).Val())
	require.Equal(t, int64(0), R.Conn.ZCard(dummyCtx, t.Name()).Val())
	require.Equal(t, int64(0), R.Conn.ZCard(dummyCtx, timer.BackupName()).Val())
}

func TestBackupTimer_EmptyReadyQueue(t *testing.T) {
	timer, err := NewTimer("test_backup_timer_set", R, time.Second, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()

	pool := NewPool(R)
	ns := "ns-test-backup"
	queueName := "q1"
	queue := NewQueue(ns, queueName, R, timer)
	count := 10
	for i := 0; i < count; i++ {
		job := engine.NewJob(ns, queueName, []byte("hello msg"+strconv.Itoa(i)), 100, 1, 3, "", "")
		pool.Add(job)
		if i%2 == 0 {
			queue.Push(job)
		} else {
			timer.Add(job.Namespace(), job.Queue(), job.ID(), job.Delay(), job.Tries())
		}
	}
	// make sure all jobs are in the ready queue and consume them without ACK,
	// so they should appear in the backup queue.
	time.Sleep(time.Second)
	assert.Equal(t, int64(count), R.Conn.ZCard(dummyCtx, timer.BackupName()).Val())
	for i := 0; i < count; i++ {
		_, err := R.Conn.BRPop(dummyCtx, time.Second, queue.Name()).Result()
		assert.Nil(t, err)
	}
	time.Sleep(2 * time.Second)
	// all jobs should requeue in ready queue
	require.Equal(t, int64(count), R.Conn.LLen(dummyCtx, queue.Name()).Val())
	require.Equal(t, int64(count), R.Conn.ZCard(dummyCtx, timer.BackupName()).Val())

	for i := 0; i < count; i++ {
		val, err := R.Conn.BRPop(dummyCtx, time.Second, queue.Name()).Result()
		assert.Nil(t, err)
		tries, jobID, err := structUnpack(val[1])
		assert.Nil(t, err)
		assert.Equal(t, uint16(3), tries)
		assert.Nil(t, pool.Delete(ns, queueName, jobID))
	}
	// backup jobs should be disappeared after jobs were ACKed
	time.Sleep(2 * time.Second)
	require.Equal(t, int64(0), R.Conn.LLen(dummyCtx, queue.Name()).Val())
	require.Equal(t, int64(0), R.Conn.ZCard(dummyCtx, t.Name()).Val())
	require.Equal(t, int64(0), R.Conn.ZCard(dummyCtx, timer.BackupName()).Val())
}

func BenchmarkTimer(b *testing.B) {
	// Disable logging temporarily
	logger.SetLevel(logrus.ErrorLevel)
	defer logger.SetLevel(logrus.DebugLevel)

	t, err := NewTimer("timer_set_3", R, time.Second, 600*time.Second)
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
			job := engine.NewJob("ns-timer", "q3", []byte("hello msg 1"), 100, 0, 1, "", "")
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
			job := engine.NewJob("ns-timer", "q3", []byte("hello msg 1"), 100, 0, 1, "", "")
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
	timer, err := NewTimer("timer_set_4", R, time.Second, 600*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	timer.Shutdown()
	for i := 0; i < 10000; i++ {
		job := engine.NewJob("ns-timer", "q4", []byte("hello msg 1"), 100, 0, 1, "", "")
		pool.Add(job)
		timer.Add(job.Namespace(), job.Queue(), job.ID(), 1, 1)
	}

	b.StartTimer()
	timer.pump(time.Now().Unix() + 1)
}

func TestScore_Encode(t *testing.T) {
	now := time.Now().Unix()
	tries := uint16(123)

	for i := 0; i < 1000; i++ {
		score := encodeScore(now+int64(i), tries)
		gotTimestamp, gotTries := decodeScore(score)
		require.Equal(t, tries, gotTries)
		require.Equal(t, now+int64(i), gotTimestamp)
	}
}
