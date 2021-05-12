package redis_v2

import (
	"bytes"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
	go_redis "github.com/go-redis/redis/v8"
)

func TestPool_Add(t *testing.T) {
	p := NewPool(R)
	job := engine.NewJob("ns-pool", "q1", []byte("hello msg 1"), 10, 0, 1)
	if err := p.Add(job); err != nil {
		t.Errorf("Failed to add job to pool: %s", err)
	}
}

// Test TTL
func TestPool_Add2(t *testing.T) {
	p := NewPool(R)
	job := engine.NewJob("ns-pool", "q2", []byte("hello msg 2"), 1, 0, 1)
	p.Add(job)
	time.Sleep(2 * time.Second)
	_, err := R.Conn.Get(dummyCtx, PoolJobKey(job)).Result()
	if err != go_redis.Nil {
		t.Fatalf("Expected to get nil result, but got: %s", err)
	}

}

func TestPool_Delete(t *testing.T) {
	p := NewPool(R)
	job := engine.NewJob("ns-pool", "q3", []byte("hello msg 3"), 10, 0, 1)
	p.Add(job)
	if err := p.Delete(job.Namespace(), job.Queue(), job.ID()); err != nil {
		t.Fatalf("Failed to delete job from pool: %s", err)
	}
}

func TestPool_Get(t *testing.T) {
	p := NewPool(R)
	job := engine.NewJob("ns-pool", "q4", []byte("hello msg 4"), 50, 0, 1)
	p.Add(job)
	body, tries, ttl, err := p.Get(job.Namespace(), job.Queue(), job.ID())
	if err != nil {
		t.Fatalf("Failed to get job: %s", err)
	}
	if tries != job.Tries() {
		t.Fatal("Mismatched job tries")
	}
	if !bytes.Equal(body, []byte("hello msg 4")) {
		t.Fatal("Mismatched job data")
	}
	if ttl > 50 || 50-ttl > 2 {
		t.Fatalf("Expected TTL is around 50 seconds")
	}
}
