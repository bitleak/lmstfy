package redis_v2

import (
	"encoding/binary"
	"time"

	"github.com/bitleak/lmstfy/engine/redis_v1"
	"github.com/bitleak/lmstfy/uuid"
	"github.com/go-redis/redis"
)

const (
	LUA_T_PUMP = `
local zset_key = KEYS[1]
local output_queue_prefix = KEYS[2]
local pool_prefix = KEYS[3]
local output_deadletter_prefix = KEYS[4]
local now = ARGV[1]
local limit = ARGV[2]
local score = ARGV[3]
local expiredMembers = redis.call("ZRANGEBYSCORE", zset_key, 0, now, "LIMIT", 0, limit)
if #expiredMembers == 0 then
	return 0
end
for _,v in ipairs(expiredMembers) do
	local ns, q, tries, job_id, priority = struct.unpack("Hc0Hc0HHc0H", v)
	if redis.call("EXISTS", table.concat({pool_prefix, ns, q, job_id}, "/")) > 0 then
		-- only pump job to ready queue/dead letter if the job did not expire
		if tries == 0 then
			-- no more tries, move to dead letter
			local val = struct.pack("HHc0H", 1, #job_id, job_id, priority)
			redis.call("PERSIST", table.concat({pool_prefix, ns, q, job_id}, "/"))  -- remove ttl
			redis.call("LPUSH", table.concat({output_deadletter_prefix, ns, q}, "/"), val)
		else
			-- move to prior ready queue
			local val = struct.pack("HHc0H", tonumber(tries), #job_id, job_id, priority)
			score = score -1
			-- 2199023255552 was 1<<41(avoid to use bit left shift)
			priority = 2199023255552 * priority + score
			redis.call("ZADD", table.concat({output_queue_prefix, ns, q}, "/"), priority, val)
		end
	end
end
redis.call("ZREM", zset_key, unpack(expiredMembers))
return #expiredMembers
`
)

// Timer is the other way of saying "delay queue". timer kick jobs into ready queue when
// it's ready.
type Timer struct {
	name     string
	redis    *redis_v1.RedisInstance
	interval time.Duration
	shutdown chan struct{}

	lua_pump_sha string
}

// NewTimer return an instance of delay queue
func NewTimer(name string, redis *redis_v1.RedisInstance, interval time.Duration) (*Timer, error) {
	timer := &Timer{
		name:     name,
		redis:    redis,
		interval: interval,
		shutdown: make(chan struct{}),
	}

	// Preload the lua scripts
	sha, err := redis.Conn.ScriptLoad(LUA_T_PUMP).Result()
	if err != nil {
		logger.WithField("err", err).Error("Failed to preload lua script in timer")
		return nil, err
	}
	timer.lua_pump_sha = sha

	go timer.tick()
	return timer, nil
}

func (t *Timer) Name() string {
	return t.name
}

func (t *Timer) Add(namespace, queue, jobID string, delaySecond uint32, tries uint16) error {
	metrics.timerAddJobs.WithLabelValues(t.redis.Name).Inc()
	timestamp := time.Now().Unix() + int64(delaySecond)

	// struct-pack the data in the format `Hc0Hc0HHc0`:
	//   {namespace len}{namespace}{queue len}{queue}{tries}{jobID len}{jobID}
	// length are 2-byte uint16 in little-endian
	namespaceLen := len(namespace)
	queueLen := len(queue)
	jobIDLen := len(jobID)
	buf := make([]byte, 2+namespaceLen+2+queueLen+2+2+jobIDLen+2)
	binary.LittleEndian.PutUint16(buf[0:], uint16(namespaceLen))
	copy(buf[2:], namespace)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen:], uint16(queueLen))
	copy(buf[2+namespaceLen+2:], queue)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen+2+queueLen:], uint16(tries))
	binary.LittleEndian.PutUint16(buf[2+namespaceLen+2+queueLen+2:], uint16(jobIDLen))
	copy(buf[2+namespaceLen+2+queueLen+2+2:], jobID)
	priority, _ := uuid.ExtractPriorityFromUniqueID(jobID)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen+2+queueLen+2+2+jobIDLen:], uint16(priority))

	return t.redis.Conn.ZAdd(t.Name(), redis.Z{Score: float64(timestamp), Member: buf}).Err()
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
	for {
		val, err := t.redis.Conn.EvalSha(
			t.lua_pump_sha,
			[]string{t.Name(), QueuePrefix, PoolPrefix, DeadLetterPrefix},
			currentSecond, batchSize, timeScore(),
		).Result()
		if err != nil {
			if isLuaScriptGone(err) { // when redis restart, the script needs to be uploaded again
				sha, err := t.redis.Conn.ScriptLoad(LUA_T_PUMP).Result()
				if err != nil {
					logger.WithField("err", err).Error("Failed to reload script")
					time.Sleep(time.Second)
					return
				}
				t.lua_pump_sha = sha
			}
			logger.WithField("err", err).Error("Failed to pump")
			time.Sleep(time.Second)
			return
		}
		n, _ := val.(int64)
		logger.WithField("count", n).Debug("Due jobs")
		metrics.timerDueJobs.WithLabelValues(t.redis.Name).Add(float64(n))
		if n == batchSize {
			// There might have more expired jobs to pump
			metrics.timerFullBatches.WithLabelValues(t.redis.Name).Inc()
			time.Sleep(10 * time.Millisecond) // Hurry up! accelerate pumping the due jobs
			continue
		}
		return
	}
}

func (t *Timer) Shutdown() {
	close(t.shutdown)
}

func (t *Timer) Size() (size int64, err error) {
	return t.redis.Conn.ZCard(t.name).Result()
}
