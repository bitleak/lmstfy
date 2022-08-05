package redis

import (
	"sync"
	"time"
)

const (
	luaPumpQueueScript = `
local zset_key = KEYS[1]
local output_queue_prefix = KEYS[2]
local pool_prefix = KEYS[3]
local stream_field_name = KEYS[4]
local now = ARGV[1]
local limit = ARGV[2]

local expiredMembers = redis.call("ZRANGEBYSCORE", zset_key, 0, now, "LIMIT", 0, limit)

if #expiredMembers == 0 then
	return 0
end

for _,v in ipairs(expiredMembers) do
	local ns, q, tries, job_id = struct.unpack("Hc0Hc0HHc0", v)
	if redis.call("EXISTS", table.concat({pool_prefix, ns, q, job_id}, "/")) > 0 then
			-- move to ready queue
			local val = struct.pack("HHc0", tonumber(tries), #job_id, job_id)
			redis.call("XADD", 'table.concat({output_queue_prefix, ns, q}, "/")', '*', 'stream_field_name', val)
		end
	end
end
redis.call("ZREM", zset_key, unpack(expiredMembers))
return #expiredMembers
`
)

// Timer manages a set of delay queues. Periodically, timer will check all delay queues
// and push due jobs into ready queue.
type Timer struct {
	name     string
	redis    *RedisInstance
	interval time.Duration
	shutdown chan struct{}
	members  map[string]struct{}
	rwmu     sync.RWMutex

	pumpSHA string
}

// NewTimer return an instance of delay queue
func NewTimer(name string, redis *RedisInstance, interval time.Duration) (*Timer, error) {
	timer := &Timer{
		name:     name,
		redis:    redis,
		interval: interval,
		shutdown: make(chan struct{}),
		members:  make(map[string]struct{}),
	}

	// todo modify luaPumpQueueScript for multiple timer sets
	sha, err := redis.Conn.ScriptLoad(dummyCtx, luaPumpQueueScript).Result()
	if err != nil {
		logger.WithField("err", err).Error("Failed to preload lua script in timer")
		return nil, err
	}
	timer.pumpSHA = sha

	go timer.tick()
	return timer, nil
}

func (t *Timer) Name() string {
	return t.name
}

// Tick pump all due jobs to the target queue
func (t *Timer) tick() {
	tick := time.NewTicker(t.interval)
	for {
		select {
		case now := <-tick.C:
			currentSecond := now.Unix()
			t.pump(currentSecond)
		case <-t.shutdown:
			return
		}
	}
}

func (t *Timer) pump(currentSecond int64) {
	var count int64
	for {
		for member := range t.members {
			val, err := t.redis.Conn.EvalSha(dummyCtx, t.pumpSHA, []string{member, QueuePrefix, PoolPrefix, StreamMessageField}, currentSecond, BatchSize).Result()
			if err != nil {
				if isLuaScriptGone(err) { // when redis restart, the script needs to be uploaded again
					sha, err := t.redis.Conn.ScriptLoad(dummyCtx, luaPumpQueueScript).Result()
					if err != nil {
						logger.WithField("err", err).Error("Failed to reload script")
						time.Sleep(time.Second)
						return
					}
					t.pumpSHA = sha
				}
				logger.WithField("err", err).Error("Failed to pump")
				time.Sleep(time.Second)
				return
			}
			n, _ := val.(int64)
			count += n
			if count >= BatchSize {
				// There might have more expired jobs to pump
				metrics.timerFullBatches.WithLabelValues(t.redis.Name).Inc()
				time.Sleep(10 * time.Millisecond) // Hurry up! accelerate pumping the due jobs
				continue
			}
		}
		logger.WithField("count", count).Debug("Due jobs")
		metrics.timerDueJobs.WithLabelValues(t.redis.Name).Add(float64(count))
		return
	}
}

func (t *Timer) Shutdown() {
	close(t.shutdown)
}

func (t *Timer) Size() (size int64, err error) {
	return t.redis.Conn.ZCard(dummyCtx, t.name).Result()
}

// AddTimerSet adds a new timer set to Timer
func (t *Timer) AddTimerSet(name string) {
	t.rwmu.Lock()
	t.members[name] = struct{}{}
	t.rwmu.Unlock()
}
