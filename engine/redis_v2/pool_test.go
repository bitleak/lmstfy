package redis_v2

import (
	"bytes"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/engine/model"

	go_redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestPool_Add(t *testing.T) {
	p := NewPool(R)
	job := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-pool",
		Queue:      "q1",
		ID:         "",
		Body:       []byte("hello msg 1"),
		TTL:        10,
		Delay:      0,
		Tries:      1,
		Attributes: nil,
	})
	if err := p.Add(job); err != nil {
		t.Errorf("Failed to add job to pool: %s", err)
	}
}

// Test TTL
func TestPool_Add2(t *testing.T) {
	p := NewPool(R)
	job := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-pool",
		Queue:      "q2",
		ID:         "",
		Body:       []byte("hello msg 2"),
		TTL:        1,
		Delay:      0,
		Tries:      1,
		Attributes: nil,
	})
	p.Add(job)
	time.Sleep(2 * time.Second)
	_, err := R.Conn.Get(dummyCtx, PoolJobKey(job)).Result()
	if err != go_redis.Nil {
		t.Fatalf("Expected to get nil result, but got: %s", err)
	}

}

func TestPool_Delete(t *testing.T) {
	p := NewPool(R)
	job := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-pool",
		Queue:      "q3",
		ID:         "",
		Body:       []byte("hello msg 3"),
		TTL:        10,
		Delay:      0,
		Tries:      1,
		Attributes: nil,
	})
	p.Add(job)
	if err := p.Delete(job.Namespace(), job.Queue(), job.ID()); err != nil {
		t.Fatalf("Failed to delete job from pool: %s", err)
	}
}

func TestPool_Get(t *testing.T) {
	p := NewPool(R)
	job := engine.NewJobFromReq(&engine.CreateJobReq{
		Namespace:  "ns-pool",
		Queue:      "q4",
		ID:         "",
		Body:       []byte("hello msg 4"),
		TTL:        50,
		Delay:      0,
		Tries:      1,
		Attributes: attributes,
	})
	p.Add(job)
	body, ttl, err := p.Get(job.Namespace(), job.Queue(), job.ID())
	if err != nil {
		t.Fatalf("Failed to get job: %s", err)
	}
	res := &model.JobData{}
	if err = proto.Unmarshal(body, res); err != nil {
		t.Fatalf("Failed to unmarshal job: %s", err)
	}
	if !bytes.Equal(res.GetData(), []byte("hello msg 4")) {
		t.Fatal("Mismatched job data")
	}
	assert.NotNil(t, res.GetAttributes())
	assert.EqualValues(t, "1", res.GetAttributes()["flag"])
	assert.EqualValues(t, "abc", res.GetAttributes()["label"])
	if ttl > 50 || 50-ttl > 2 {
		t.Fatalf("Expected TTL is around 50 seconds")
	}
}
