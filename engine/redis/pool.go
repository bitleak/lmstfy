package redis

import (
	"encoding/json"
	"errors"
	"time"

	go_redis "github.com/go-redis/redis/v8"

	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/uuid"
)

type JobPayload struct {
	Body       []byte            `json:"body"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Pool stores all the jobs' data. this is a global singleton per engine
// note: this `Pool` is NOT the same terminology as the EnginePool
type Pool struct {
	redis *RedisInstance
}

func NewPool(redis *RedisInstance) *Pool {
	return &Pool{
		redis: redis,
	}
}

func PoolJobKey(j engine.Job) string {
	return join(PoolPrefix, j.Namespace(), j.Queue(), j.ID())
}

func PoolJobKey2(namespace, queue, jobID string) string {
	return join(PoolPrefix, namespace, queue, jobID)
}

func PoolJobKeyPrefix(namespace, queue string) string {
	return join(PoolPrefix, namespace, queue)
}

func (p *Pool) Add(j engine.Job) (err error) {
	metrics.poolAddJobs.WithLabelValues(p.redis.Name).Inc()

	// For the version 0(legacy) jobID, the payload is the body directly,
	// for the version 1 jobID, the payload is a JSON string contains the body.
	payload := j.Body()
	if uuid.ExtractJobIDVersion(j.ID()) != 0 {
		payload, err = json.Marshal(JobPayload{Body: j.Body(), Attributes: j.Attributes()})
		if err != nil {
			return err
		}
	}

	// SetNX return OK(true) if key didn't exist before.
	ok, err := p.redis.Conn.SetNX(dummyCtx, PoolJobKey(j), payload, time.Duration(j.TTL())*time.Second).Result()
	if err != nil {
		// Just retry once.
		ok, err = p.redis.Conn.SetNX(dummyCtx, PoolJobKey(j), payload, time.Duration(j.TTL())*time.Second).Result()
	}
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("key existed") // Key existed before, avoid overwriting it, so return error
	}
	return err
}

func (p *Pool) Get(namespace, queue, jobID string) (*JobPayload, uint32, error) {
	pipeline := p.redis.Conn.Pipeline()
	jobKey := join(PoolPrefix, namespace, queue, jobID)
	getCmd := pipeline.Get(dummyCtx, jobKey)
	ttlCmd := pipeline.TTL(dummyCtx, jobKey)
	_, err := pipeline.Exec(dummyCtx)
	if err != nil {
		if errors.Is(err, go_redis.Nil) {
			return nil, 0, engine.ErrNotFound
		}
		return nil, 0, err
	}

	val := []byte(getCmd.Val())
	ttl := int64(ttlCmd.Val().Seconds())
	if ttl < 0 {
		// Use `0` to identify indefinite TTL, NOTE: in redis ttl=0 is possible when
		// the key is not recycled fast enough. but here is okay we use `0` to identify
		// indefinite TTL, because we issue GET cmd before TTL cmd, so the ttl must be > 0,
		// OR GET cmd would fail.
		ttl = 0
	}
	metrics.poolGetJobs.WithLabelValues(p.redis.Name).Inc()

	if uuid.ExtractJobIDVersion(jobID) == 0 {
		// For the version 0(legacy) jobID, the val only contains the body,
		// so we need to return the val as body directly.
		return &JobPayload{Body: val}, uint32(ttl), nil
	}

	// For the version 1 jobID, the value is encoded as a JSON string,
	// need to unmarshal it before return.
	var payload JobPayload
	if err := json.Unmarshal(val, &payload); err != nil {
		return nil, 0, err
	}
	return &payload, uint32(ttl), nil
}

func (p *Pool) Delete(namespace, queue, jobID string) error {
	metrics.poolDeleteJobs.WithLabelValues(p.redis.Name).Inc()
	return p.redis.Conn.Del(dummyCtx, join(PoolPrefix, namespace, queue, jobID)).Err()
}
