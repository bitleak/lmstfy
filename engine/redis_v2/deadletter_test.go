package redis_v2

import (
	"fmt"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
)

func TestDeadLetterV2_Add(t *testing.T) {
	dl, _ := NewDeadLetter("ns-dead", "q0", R)
	if err := dl.Add("x"); err != nil {

	}
}

func TestDeadLetterV2_Peek(t *testing.T) {
	dl, _ := NewDeadLetter("ns-dead", "q1", R)
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

func TestDeadLetterV2_Delete(t *testing.T) {
	dl, _ := NewDeadLetter("ns-dead", "q2", R)
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

func TestDeadLetterV2_Respawn(t *testing.T) {
	p := NewPool(R)
	job1 := engine.NewJob("ns-dead", "q3", []byte("1"), 60, 0, 1, 3)
	job2 := engine.NewJob("ns-dead", "q3", []byte("2"), 60, 0, 1, 2)
	job3 := engine.NewJob("ns-dead", "q3", []byte("3"), 60, 0, 1, 1)
	p.Add(job1)
	p.Add(job2)
	p.Add(job3)
	dl, _ := NewDeadLetter("ns-dead", "q3", R)
	dl.Add(job1.ID())
	dl.Add(job2.ID())
	dl.Add(job3.ID())

	// Ensure TTL is removed when put into deadletter
	job1Key := PoolJobKey(job1)
	job1TTL := R.Conn.TTL(job1Key).Val()
	if job1TTL.Seconds() > 0 {
		t.Fatalf("Respawned job's TTL should be removed")
	}

	timer, err := NewTimer("ns-dead", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue("ns-dead", "q3", R, timer)

	count, err := dl.Respawn(2, 10)
	if err != nil || count != 2 {
		t.Fatalf("Failed to respawn two jobs: %s", err)
	}
	jobID, _, err := q.Poll(1, 1)
	if err != nil || jobID != job1.ID() {
		t.Fatal("Expected to poll the first job respawned from deadletter")
	}
	// Ensure TTL is set
	job1Key = PoolJobKey(job1)
	job1TTL = R.Conn.TTL(job1Key).Val()
	if 10-job1TTL.Seconds() > 2 { // 2 seconds passed? no way.
		t.Fatal("Deadletter job's TTL is not correct")
	}
	q.Poll(1, 1) // rm job2

	count, err = dl.Respawn(1, 10)
	if err != nil || count != 1 {
		t.Fatalf("Failed to respawn one jobs: %s", err)
	}
	jobID, _, err = q.Poll(1, 1)
	if err != nil || jobID != job3.ID() {
		t.Fatal("Expected to poll the second job respawned from deadletter")
	}

	// Ensure TTL is set
	job2Key := PoolJobKey(job2)
	job2TTL := R.Conn.TTL(job2Key).Val()
	if 10-job2TTL.Seconds() > 2 {
		t.Fatal("Deadletter job's TTL is not correct")
	}
}

func TestDeadLetterV2_Size(t *testing.T) {
	p := NewPool(R)
	dl, _ := NewDeadLetter("ns-dead", "q3", R)
	cnt := 3
	for i := 0; i < cnt; i++ {
		job := engine.NewJob("ns-dead", "q3", []byte("1"), 60, 0, 1, 0)
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

func TestDeadLetterWithPriorQueueV2_Respawn(t *testing.T) {
	p := NewPool(R)
	namespace := "ns-deadletter-prior-queue"
	queue := "q1"
	dl, _ := NewDeadLetter(namespace, queue, R)
	jobIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		job := engine.NewJob(namespace, queue, []byte("1"), 60, 0, 1, uint8(i))
		jobIDs[i] = job.ID()
		p.Add(job)
		dl.Add(job.ID())
		// Ensure TTL is removed when put into deadletter
		jobKey := PoolJobKey(job)
		jobTTL := R.Conn.TTL(jobKey).Val()
		if jobTTL.Seconds() > 0 {
			t.Fatalf("Respawned job's TTL should be removed")
		}
	}

	timer, err := NewTimer("ns-dead-prior-queue-timer", R, time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to new timer: %s", err))
	}
	defer timer.Shutdown()
	q := NewQueue(namespace, queue, R, timer)

	count, err := dl.Respawn(3, 10)
	if err != nil || count != 3 {
		t.Fatalf("Failed to respawn two jobs: %s", err)
	}

	members, err := R.Conn.ZRangeWithScores(q.Name(), 0, -1).Result()
	if err != nil {
		t.Fatal("Failed to zrange the ready queue")
	}
	if len(members) != 3 {
		t.Fatalf("Ready queue size 10 was expected but got %d", len(members))
	}
	for i, member := range members { // this case was used to make sure that score in ready queue was right
		if i != int(member.Score) {
			t.Fatalf("Invalid ready queue priority, %d was expected but got %d", i, int(member.Score))
		}
	}

	for i := 0; i < 3; i++ {
		jobID, _, err := q.Poll(1, 1)
		if err != nil || jobID != jobIDs[2-i] {
			t.Fatalf("Mismatch job id, %s was expected but got %s", jobIDs[2-i], jobID)
		}
		// Ensure TTL is set
		jobKey := join(PoolPrefix, namespace, queue, jobID)
		jobTTL := R.Conn.TTL(jobKey).Val()
		if 10-jobTTL.Seconds() > 2 { // 2 seconds passed? no way.
			t.Fatal("Deadletter job's TTL is not correct")
		}
	}

	job := engine.NewJob(namespace, queue, []byte("1"), 60, 0, 1, uint8(2))
	p.Add(job)
	dl.Add(job.ID())
	count, err = dl.Respawn(1, 10)
	if err != nil || count != 1 {
		t.Fatalf("Failed to respawn one jobs: %s", err)
	}
	jobID, _, err := q.Poll(1, 1)
	if err != nil || jobID != job.ID() {
		t.Fatalf("Mismatch job id, %s was expected but got %s", job.ID(), jobID)
	}

	// Ensure TTL is set
	jobKey := join(PoolPrefix, namespace, queue, jobID)
	jobTTL := R.Conn.TTL(jobKey).Val()
	if 10-jobTTL.Seconds() > 2 {
		t.Fatal("Deadletter job's TTL is not correct")
	}
}
