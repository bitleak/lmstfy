package redis_v2

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/uuid"
)

func TestTimerManager_Candidate(t *testing.T) {
	queueManager, err := NewQueueManager(R)
	if err != nil {
		t.Fatal("init queue manager error", err)
		return
	}
	defer queueManager.Close()
	timerManager, err := NewTimerManager(queueManager, R)
	if err != nil {
		t.Fatal("init timer manager error", err)
		return
	}
	defer timerManager.Close()

	for i := 0; i < 5; i++ {
		deadlineStr, err := R.Conn.HGet(dummyCtx, TimerManagerInstanceSetKey, timerManager.id).Result()
		if err != nil {
			t.Fatal("get timer manager register deadline error", err)
			return
		}
		deadline, _ := strconv.ParseInt(deadlineStr, 10, 64)
		if time.Now().Unix() > deadline {
			t.Fatal("timer manager register has no effect")
			return
		}
		time.Sleep(TimerManagerInstanceCheckInterval * time.Second)
	}
}

func TestTimerManager_Elect(t *testing.T) {
	queueManager, err := NewQueueManager(R)
	if err != nil {
		t.Fatal("init queue manager error", err)
		return
	}
	defer queueManager.Close()
	timerManager, err := NewTimerManager(queueManager, R)
	if err != nil {
		t.Fatal("init timer manager error", err)
		return
	}
	defer timerManager.Close()

	if timerManager.sequence != TimerManagerMasterSequence {
		t.Fatal("expect timer manager first sequence number is 0, but got", timerManager.sequence)
		return
	}

	time.Sleep(time.Second)
	timerManager2, err := NewTimerManager(queueManager, R)
	if err != nil {
		t.Fatal("init timer manager error", err)
		return
	}
	defer timerManager2.Close()

	if timerManager2.sequence != 1 {
		t.Fatal("expect timer manager 2 sequence number is 1, but got", timerManager2.sequence)
		return
	}
	time.Sleep(TimerManagerInstanceCheckInterval * time.Second)
	if timerManager.sequence != TimerManagerMasterSequence {
		t.Fatal("expect timer manager sequence number is 0, but got", timerManager.sequence)
		return
	}

	// add an early dummy timer manager manual
	R.Conn.HSet(dummyCtx, TimerManagerInstanceSetKey, fmt.Sprintf("{%d}-{dummyManager}-{%s}",
		time.Now().Add(-time.Minute).UnixNano()/int64(time.Millisecond), uuid.GenUniqueID()), time.Now().Unix()+3)
	time.Sleep(TimerManagerInstanceCheckInterval*time.Second + 100*time.Millisecond)

	if timerManager.sequence != 1 {
		t.Fatal("expect timer manager 1 sequence number is 1, but got", timerManager.sequence)
		return
	}
	if timerManager2.sequence != 2 {
		t.Fatal("expect timer manager 2 sequence number is 2, but got", timerManager2.sequence)
		return
	}

	time.Sleep(2 * TimerManagerInstanceCheckInterval * time.Second)
	if timerManager.sequence != 0 {
		t.Fatal("expect timer manager 1 sequence number is 0, but got", timerManager.sequence)
		return
	}
	if timerManager2.sequence != 1 {
		t.Fatal("expect timer manager 2 sequence number is 1, but got", timerManager2.sequence)
		return
	}
}

func TestTimerManager_Add(t *testing.T) {
	queueManager, err := NewQueueManager(R)
	if err != nil {
		t.Fatal("init queue manager error", err)
		return
	}
	defer queueManager.Close()
	timerManager, err := NewTimerManager(queueManager, R)
	if err != nil {
		t.Fatal("init timer manager error", err)
		return
	}
	defer timerManager.Close()

	job := engine.NewJob("ns-timer", "q1", []byte("hello msg 1"), 10, 0, 1)
	if err = timerManager.Add(job.Namespace(), job.Queue(), job.ID(), 10); err != nil {
		t.Errorf("Failed to add job to timer: %s", err)
	}
}

func TestTimerManager_Pump(t *testing.T) {
	queueManager, err := NewQueueManager(R)
	if err != nil {
		t.Fatal("init queue manager error", err)
		return
	}
	defer queueManager.Close()
	err = queueManager.Add("ns-timer", "q2")
	if err != nil {
		t.Fatal("add queue error", err)
		return
	}
	timerManager, err := NewTimerManager(queueManager, R)
	if err != nil {
		t.Fatal("init timer manager error", err)
		return
	}
	defer timerManager.Close()

	job := engine.NewJob("ns-timer", "q2", []byte("hello msg 2"), 5, 0, 1)
	R.Conn.HMSet(dummyCtx, join(PoolPrefix, job.Namespace(), job.Queue(), job.ID()), "data", job.Body(), "tries", job.Tries())
	err = timerManager.Add(job.Namespace(), job.Queue(), job.ID(), 3)
	if err != nil {
		t.Fatal("timer manager add job error", err)
		return
	}
	wait := make(chan struct{})
	go func() {
		defer func() {
			wait <- struct{}{}
		}()
		val, err := R.Conn.BRPop(dummyCtx, 5*time.Second, join(ReadyQueuePrefix, "ns-timer", "q2")).Result()
		if err != nil || len(val) == 0 {
			t.Fatal("Failed to pop the job from target queue")
		}
		if val[1] != job.ID() {
			t.Fatal("Job data mismatched")
		}
	}()
	<-wait
}

// todo: benchmark test
