package redis_v2

import (
	"errors"
	"strconv"
	"time"

	go_redis "github.com/go-redis/redis/v8"

	"github.com/bitleak/lmstfy/engine"
)

const (
	PoolJobFieldData  = "data"
	PoolJobFieldTries = "tries"
)

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

func PoolJobKey2(q queue, jobID string) string {
	return join(PoolPrefix, q.namespace, q.queue, jobID)
}

func PoolJobKey3(namespace, queue, jobID string) string {
	return join(PoolPrefix, namespace, queue, jobID)
}

func (p *Pool) Add(j engine.Job) error {
	body := j.Body()
	jobKey := PoolJobKey(j)
	metrics.poolAddJobs.WithLabelValues(p.redis.Name).Inc()
	// HSetNX return OK(true) if key didn't exist before.
	pipeline := p.redis.Conn.Pipeline()
	dataCmd := pipeline.HSetNX(dummyCtx, jobKey, PoolJobFieldData, body)
	triesCmd := pipeline.HSetNX(dummyCtx, jobKey, PoolJobFieldTries, j.Tries())
	_, err := pipeline.Exec(dummyCtx)
	if err != nil {
		return err
	}
	if !dataCmd.Val() || !triesCmd.Val() {
		return errors.New("key existed") // Key existed before, avoid overwriting it, so return error
	}
	// add is not atomic because expire command exec after hsetnx
	return p.redis.Conn.Expire(dummyCtx, jobKey, time.Duration(j.TTL())*time.Second).Err()
}

func (p *Pool) Get(namespace, queue, jobID string) (body []byte, tries uint16, ttlSecond uint32, err error) {
	pipeline := p.redis.Conn.Pipeline()
	jobKey := PoolJobKey3(namespace, queue, jobID)
	getCmd := pipeline.HMGet(dummyCtx, jobKey, PoolJobFieldData, PoolJobFieldTries)
	ttlCmd := pipeline.TTL(dummyCtx, jobKey)
	_, err = pipeline.Exec(dummyCtx)
	switch err {
	case nil:
		val := getCmd.Val()
		if len(val) != 2 {
			return nil, 0, 0, errors.New("pool hmget should return 2 interface")
		}
		data, ok := val[0].(string)
		if !ok {
			return nil, 0, 0, errors.New("body should be []byte")
		}
		triesStr, ok := val[1].(string)
		if !ok {
			return nil, 0, 0, errors.New("tries should be string")
		}
		tries, err := strconv.ParseUint(triesStr, 10, 16)
		if err != nil {
			return nil, 0, 0, errors.New("tries convert string to uint16 error")
		}
		ttl := int64(ttlCmd.Val().Seconds())
		if ttl < 0 {
			// Use `0` to identify indefinite TTL, NOTE: in redis ttl=0 is possible when
			// the key is not recycled fast enough. but here is okay we use `0` to identify
			// indefinite TTL, because we issue GET cmd before TTL cmd, so the ttl must be > 0,
			// OR GET cmd would fail.
			ttl = 0
		}
		metrics.poolGetJobs.WithLabelValues(p.redis.Name).Inc()
		return []byte(data), uint16(tries), uint32(ttl), nil
	case go_redis.Nil:
		return nil, 0, 0, engine.ErrNotFound
	default:
		return nil, 0, 0, err
	}
}

func (p *Pool) Delete(namespace, queue, jobID string) error {
	metrics.poolDeleteJobs.WithLabelValues(p.redis.Name).Inc()
	return p.redis.Conn.Del(dummyCtx, PoolJobKey3(namespace, queue, jobID)).Err()
}
