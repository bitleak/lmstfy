package redis

import (
	"encoding/json"
	"io"
	"time"

	go_redis "github.com/go-redis/redis/v8"

	"github.com/bitleak/lmstfy/engine"
)

type RedisInstance struct {
	Name string
	Conn *go_redis.Client
}

// Engine that connects all the dots including:
// - store jobs to timer set or ready queue
// - deliver jobs to clients
// - manage dead letters
type Engine struct {
	redis   *RedisInstance
	pool    *Pool
	timer   *Timer
	meta    *MetaManager
	monitor *SizeMonitor
}

func NewEngine(redisName string, conn *go_redis.Client) (engine.Engine, error) {
	redis := &RedisInstance{
		Name: redisName,
		Conn: conn,
	}
	if err := PreloadDeadLetterLuaScript(redis); err != nil {
		return nil, err
	}
	if err := PreloadQueueLuaScript(redis); err != nil {
		return nil, err
	}
	go RedisInstanceMonitor(redis)
	meta := NewMetaManager(redis)
	timer, err := NewTimer("timer_set", redis, time.Second)
	if err != nil {
		return nil, err
	}
	metadata, err := meta.Dump()
	if err != nil {
		return nil, err
	}
	monitor := NewSizeMonitor(redis, timer, metadata)
	go monitor.Loop()
	return &Engine{
		redis:   redis,
		pool:    NewPool(redis),
		timer:   timer,
		meta:    meta,
		monitor: monitor,
	}, nil
}

func (e *Engine) Queue(meta engine.QueueMeta) (engine.Queue, error) {
	return &Queue{
		meta: meta,
		e:    e,
		// NOTE: deadletter and queue are actually the same data structure, we could reuse the lua script
		// to empty the redis list (used as queue here). all we need to do is pass the queue name as the
		// deadletter name.
		destroySHA: deleteDeadletterSHA,
	}, nil
}

func (e *Engine) Queues(metas []engine.QueueMeta) (engine.Queues, error) {
	return &Queues{
		meta: metas,
		e:    e,
	}, nil
}
func (e *Engine) DeadLetter(meta engine.QueueMeta) (engine.DeadLetter, error) {
	return &DeadLetter{
		meta:  meta,
		redis: e.redis,
	}, nil
}

func (e *Engine) Shutdown() {
	e.timer.Shutdown()
}

func (e *Engine) DumpInfo(out io.Writer) error {
	metadata, err := e.meta.Dump()
	if err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "    ")
	return enc.Encode(metadata)
}
