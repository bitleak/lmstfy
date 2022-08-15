package redis_v2

import (
	"encoding/binary"
	"math"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	luaPumpQueueScript = `
local zset_key = KEYS[1]
local output_queue_prefix = KEYS[2]
local pool_prefix = KEYS[3]
local output_deadletter_prefix = KEYS[4]
local min_score= ARGV[1]
local limit = ARGV[2]

local expiredMembers = redis.call("ZRANGEBYSCORE", zset_key, 0, min_score, "WITHSCORES", "LIMIT", 0, limit)

if #expiredMembers == 0 then
	return 0
end

local toBeRemovedMembers = {}
for i = 1, #expiredMembers, 2 do
	local v = expiredMembers[i]
	table.insert(toBeRemovedMembers, v)
	local score = tonumber(expiredMembers[i+1])
	local tries = bit.band(score, 0xffff)
	local ns, q, job_id = struct.unpack("Hc0Hc0Hc0", v)
	if redis.call("EXISTS", table.concat({pool_prefix, ns, q, job_id}, "/")) > 0 then
		-- only pump job to ready queue/dead letter if the job did not expire
		if tries == 0 then
			-- no more tries, move to dead letter
			local val = struct.pack("HHc0", 1, #job_id, job_id)
			redis.call("PERSIST", table.concat({pool_prefix, ns, q, job_id}, "/"))  -- remove ttl
			redis.call("LPUSH", table.concat({output_deadletter_prefix, ns, q}, "/"), val)
		else
			-- move to ready queue
			local val = struct.pack("HHc0", tonumber(tries), #job_id, job_id)
			redis.call("LPUSH", table.concat({output_queue_prefix, ns, q}, "/"), val)
			local backup_key = table.concat({output_queue_prefix, ns, q, "backup"}, "/")
			redis.call("ZADD", backup_key, score, job_id)
		end
	end
end
redis.call("ZREM", zset_key, unpack(toBeRemovedMembers))
return #expiredMembers
`
)

// Timer is the other way of saying "delay queue". timer kick jobs into ready queue when
// it's ready.
type Timer struct {
	name     string
	redis    *RedisInstance
	interval time.Duration
	shutdown chan struct{}

	pumpSHA string
}

// NewTimer return an instance of delay queue
func NewTimer(name string, redis *RedisInstance, interval time.Duration) (*Timer, error) {
	timer := &Timer{
		name:     name,
		redis:    redis,
		interval: interval,
		shutdown: make(chan struct{}),
	}

	// Preload the lua scripts
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

func encodeScore(timestamp int64, tries uint16) float64 {
	return float64(((timestamp & 0xffffffff) << 16) | int64(tries&0xffff))
}

func decodeScore(score float64) (int64, uint16) {
	val := int64(score)
	timestamp := (val >> 16) & 0xffffffff
	tries := uint16(val & 0xffff)
	return timestamp, tries
}

func (t *Timer) Add(namespace, queue, jobID string, delaySecond uint32, tries uint16) error {
	metrics.timerAddJobs.WithLabelValues(t.redis.Name).Inc()
	timestamp := time.Now().Unix() + int64(delaySecond)

	score := encodeScore(timestamp, tries)
	// struct-pack the data in the format `Hc0Hc0Hc0`:
	//   {namespace len}{namespace}{queue len}{queue}{jobID len}{jobID}
	// length are 2-byte uint16 in little-endian
	namespaceLen := len(namespace)
	queueLen := len(queue)
	jobIDLen := len(jobID)
	buf := make([]byte, 2+namespaceLen+2+queueLen+2+2+jobIDLen)
	binary.LittleEndian.PutUint16(buf[0:], uint16(namespaceLen))
	copy(buf[2:], namespace)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen:], uint16(queueLen))
	copy(buf[2+namespaceLen+2:], queue)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen+2+queueLen:], uint16(jobIDLen))
	copy(buf[2+namespaceLen+2+queueLen+2:], jobID)

	err := t.redis.Conn.ZAdd(dummyCtx, t.Name(), &redis.Z{Score: score, Member: buf}).Err()
	if err != nil {
		return err
	}

	_ = t.removeFromBackup(namespace, queue, jobID)
	return nil
}

func (t *Timer) BuildBackupKey(namespace, queue string) string {
	return strings.Join([]string{QueuePrefix, namespace, queue, "backup"}, "/")
}

func (t *Timer) addToBackup(namespace, queue, jobID string, tries uint16) error {
	score := encodeScore(time.Now().Unix(), tries)
	backupQueueName := t.BuildBackupKey(namespace, queue)
	return t.redis.Conn.ZAdd(dummyCtx, backupQueueName, &redis.Z{Score: score, Member: jobID}).Err()
}

func (t *Timer) removeFromBackup(namespace, queue, jobID string) error {
	metrics.timerRemoveBackupJobs.WithLabelValues(t.redis.Name).Inc()
	backupQueueName := t.BuildBackupKey(namespace, queue)
	return t.redis.Conn.ZRem(dummyCtx, backupQueueName, jobID).Err()
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
	minScore := encodeScore(currentSecond, math.MaxUint16)
	for {
		val, err := t.redis.Conn.EvalSha(dummyCtx, t.pumpSHA, []string{t.Name(), QueuePrefix, PoolPrefix, DeadLetterPrefix}, minScore, BatchSize).Result()
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
		logger.WithField("count", n).Debug("Due jobs")
		metrics.timerDueJobs.WithLabelValues(t.redis.Name).Add(float64(n))
		if n == BatchSize {
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
	return t.redis.Conn.ZCard(dummyCtx, t.name).Result()
}
