package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
)

func TestDeadLetter_Add(t *testing.T) {
	meta := engine.QueueMeta{
		Namespace: "ns-dead",
		Queue:     "q0",
	}
	dl := DeadLetter{meta: meta, redis: R}
	if err := dl.Add("x"); err != nil {

	}
}

func TestDeadLetter_Peek(t *testing.T) {
	meta := engine.QueueMeta{
		Namespace: "ns-dead",
		Queue:     "q1",
	}
	dl := DeadLetter{meta: meta, redis: R}
	dl.Add("x")
	dl.Add("y")
	dl.Add("z")

	size, jobID, err := dl.Peek()
	if err != nil {
		t.Fatalf("Failed to peek deadletter: %s", err)
	}
	if size != 3 || jobID != "x" {
		t.Fatal("Mismatched job")
	}
}

func TestDeadLetter_Delete(t *testing.T) {
	meta := engine.QueueMeta{
		Namespace: "ns-dead",
		Queue:     "q2",
	}
	dl := DeadLetter{meta: meta, redis: R}
	dl.Add("x")
	dl.Add("y")
	dl.Add("z")

	count, err := dl.Delete(2)
	if err != nil || count != 2 {
		t.Fatalf("Failed to delete two jobs from deadletter")
	}
	size, jobID, _ := dl.Peek()
	if size != 1 || jobID != "z" {
		t.Fatal("Expected two jobs in deadletter")
	}

	count, err = dl.Delete(1)
	if err != nil || count != 1 {
		t.Fatalf("Failed to delete job from deadletter")
	}
	size, jobID, _ = dl.Peek()
	if size != 0 {
		t.Fatal("Expected no job in deadletter")
	}
}

func TestDeadLetter_Respawn(t *testing.T) {
	meta := engine.QueueMeta{
		Namespace: "ns-dead",
		Queue:     "q3",
	}

	p := NewPool(R)
	job1 := engine.NewJob(meta, []byte("1"), 60, 0, 1)
	job2 := engine.NewJob(meta, []byte("2"), 60, 0, 1)
	job3 := engine.NewJob(meta, []byte("3"), 60, 0, 1)
	p.Add(job1)
	p.Add(job2)
	p.Add(job3)
	dl := DeadLetter{meta: meta, redis: R}
	dl.Add(job1.ID())
	dl.Add(job2.ID())
	dl.Add(job3.ID())

	// Ensure TTL is removed when put into deadletter
	job1Key := PoolJobKey(job1)
	job1TTL := R.Conn.TTL(dummyCtx, job1Key).Val()
	if job1TTL.Seconds() > 0 {
		t.Fatalf("Respawned job's TTL should be removed")
	}

	timer, err := NewTimer("ns-dead", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()

	count, err := dl.Respawn(2, 10)
	if err != nil || count != 2 {
		t.Fatalf("Failed to respawn two jobs: %s", err)
	}
	_, jobID, _, err := pollReady(E.(*Engine), []engine.QueueMeta{meta}, 1, 1)
	if err != nil || jobID != job1.ID() {
		t.Fatal("Expected to poll the first job respawned from deadletter")
	}
	// Ensure TTL is set
	job1Key = PoolJobKey(job1)
	job1TTL = R.Conn.TTL(dummyCtx, job1Key).Val()
	if 10-job1TTL.Seconds() > 2 { // 2 seconds passed? no way.
		t.Fatal("Deadletter job's TTL is not correct")
	}
	pollReady(E.(*Engine), []engine.QueueMeta{meta}, 1, 1)

	count, err = dl.Respawn(1, 10)
	if err != nil || count != 1 {
		t.Fatalf("Failed to respawn one jobs: %s", err)
	}
	_, jobID, _, err = pollReady(E.(*Engine), []engine.QueueMeta{meta}, 1, 1)
	if err != nil || jobID != job3.ID() {
		t.Fatal("Expected to poll the second job respawned from deadletter")
	}

	// Ensure TTL is set
	job2Key := PoolJobKey(job2)
	job2TTL := R.Conn.TTL(dummyCtx, job2Key).Val()
	if 10-job2TTL.Seconds() > 2 {
		t.Fatal("Deadletter job's TTL is not correct")
	}
}

func TestDeadLetter_Size(t *testing.T) {
	p := NewPool(R)
	meta := engine.QueueMeta{
		Namespace: "ns-dead",
		Queue:     "q3",
	}
	dl := DeadLetter{meta: meta, redis: R}
	cnt := 3
	for i := 0; i < cnt; i++ {
		job := engine.NewJob(meta, []byte("1"), 60, 0, 1)
		p.Add(job)
		dl.Add(job.ID())
	}
	size, _ := dl.Size()
	if size != int64(cnt) {
		t.Fatalf("Expected the deadletter queue size is: %d, but got %d\n", cnt, size)
	}
	dl.Delete(3)
	size, _ = dl.Size()
	if size != 0 {
		t.Fatalf("Expected the deadletter queue size is: %d, but got %d\n", 0, size)
	}
}
