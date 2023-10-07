package redis_v2

import (
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	luaPumpBackupScript = `
local zset_key = KEYS[1]
local queue_prefix = KEYS[2]
local pool_prefix = KEYS[3]
local new_score = tonumber(ARGV[1])
local max_score = ARGV[2]
local limit = ARGV[3]
local backup_key = table.concat({zset_key, "backup"}, "/")

local memberScores = redis.call("ZRANGEBYSCORE", backup_key, 0, max_score, "WITHSCORES", "LIMIT", 0, limit)

if #memberScores == 0 then
	return 0
end

local toBeRemovedMembers = {}
for i = 1, #memberScores, 2 do
	local member = memberScores[i]
	local score = tonumber(memberScores[i+1])
	local ns, q, job_id = struct.unpack("Hc0Hc0Hc0", member)
	local need_next_check = true
	-- ignore jobs which are expired or ACKed
	if redis.call("EXISTS", table.concat({pool_prefix, ns, q, job_id}, "/")) == 0 then
		-- the job was expired or acked, just discard it
		table.insert(toBeRemovedMembers, member)
		need_next_check = false
	end

	local oldest_elem = nil
	if need_next_check then
		oldest_elem = redis.call("LINDEX", table.concat({queue_prefix, ns, q}, "/"), "-1")
		if not oldest_elem then
			-- no elem in the queue means those queue jobs in backup are lost,
			-- since consumer can't fetch them.
			table.insert(toBeRemovedMembers, member)
			redis.call("ZADD", zset_key, score, member)
			need_next_check = false
		end
	end

	if need_next_check then
		-- the score in backup is updated by queuing time, so it means those jobs
		-- should be consumed since they are older than first element in ready queue
		local tries, oldest_job_id = struct.unpack("HHc0", oldest_elem)
		local oldest_member = struct.pack("Hc0Hc0Hc0", #ns, ns, #q, q, #oldest_job_id, oldest_job_id)
		local oldest_score = redis.call("ZSCORE", backup_key, oldest_member)
		if oldest_score and tonumber(oldest_score) > score then
			table.insert(toBeRemovedMembers, member)
			local tries = bit.band(score, 0xffff)
			redis.call("ZADD", zset_key, new_score+tries, member)
		end
	end
end

if #toBeRemovedMembers > 0 then
	redis.call("ZREM", backup_key, unpack(toBeRemovedMembers))
end
return #toBeRemovedMembers
`
	luaPumpQueueScript = `
local zset_key = KEYS[1]
local output_queue_prefix = KEYS[2]
local pool_prefix = KEYS[3]
local output_deadletter_prefix = KEYS[4]
local max_score= ARGV[1]
local limit = ARGV[2]

local backup_key = table.concat({zset_key, "backup"}, "/")
local expiredMembers = redis.call("ZRANGEBYSCORE", zset_key, 0, max_score, "WITHSCORES", "LIMIT", 0, limit)

if #expiredMembers == 0 then
	return 0
end

-- we want to remove those members after pumping into ready queue,
-- so need a new array to record members without score.
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
			redis.call("ZREM", backup_key, v)
		else
			-- move to ready queue
			local val = struct.pack("HHc0", tonumber(tries), #job_id, job_id)
			redis.call("LPUSH", table.concat({output_queue_prefix, ns, q}, "/"), val)
			redis.call("ZADD", backup_key, score, v)
		end
	else
		-- the job was expired or acked, just discard it
		redis.call("ZREM", backup_key, v)
	end
end
redis.call("ZREM", zset_key, unpack(toBeRemovedMembers))
return #toBeRemovedMembers
`
)

// Timer is the other way of saying "delay queue". timer kick jobs into ready queue when
// it's ready.
type Timer struct {
	name                string
	redis               *RedisInstance
	interval            time.Duration
	checkBackupInterval time.Duration
	shutdown            chan struct{}

	pumpSHA       string
	pumpBackupSHA string
}

// NewTimer return an instance of delay queue
func NewTimer(name string, redis *RedisInstance, interval, checkBackupInterval time.Duration) (*Timer, error) {
	timer := &Timer{
		name:                name,
		redis:               redis,
		interval:            interval,
		checkBackupInterval: checkBackupInterval,
		shutdown:            make(chan struct{}),
	}

	// Preload the lua scripts
	sha, err := redis.Conn.ScriptLoad(dummyCtx, luaPumpQueueScript).Result()
	if err != nil {
		logger.WithField("err", err).Error("Failed to preload lua script in timer")
		return nil, err
	}
	timer.pumpSHA = sha

	backupSHA, err := redis.Conn.ScriptLoad(dummyCtx, luaPumpBackupScript).Result()
	if err != nil {
		logger.WithField("err", err).Error("Failed to preload lua script in timer")
		return nil, err
	}
	timer.pumpBackupSHA = backupSHA

	go timer.tick()
	return timer, nil
}

func (t *Timer) Name() string {
	return t.name
}

// encodeScore will encode timestamp(unix second) and tries as score.
// Be careful that the underlay of Lua number is double, so it will
// lose precious if overrun the 53bit.
func encodeScore(timestamp int64, tries uint16) float64 {
	return float64(((timestamp & 0xffffffff) << 16) | int64(tries&0xffff))
}

// decodeScore will decode the score into timestamp and tries
func decodeScore(score float64) (int64, uint16) {
	val := int64(score)
	timestamp := (val >> 16) & 0xffffffff
	tries := uint16(val & 0xffff)
	return timestamp, tries
}

// structPackTimerData will struct-pack the data in the format `Hc0Hc0Hc0`:
//
//	{namespace len}{namespace}{queue len}{queue}{jobID len}{jobID}
//
// length are 2-byte uint16 in little-endian
func structPackTimerData(namespace, queue, jobID string) []byte {
	namespaceLen := len(namespace)
	queueLen := len(queue)
	jobIDLen := len(jobID)
	buf := make([]byte, 2+namespaceLen+2+queueLen+2+jobIDLen)
	binary.LittleEndian.PutUint16(buf[0:], uint16(namespaceLen))
	copy(buf[2:], namespace)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen:], uint16(queueLen))
	copy(buf[2+namespaceLen+2:], queue)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen+2+queueLen:], uint16(jobIDLen))
	copy(buf[2+namespaceLen+2+queueLen+2:], jobID)
	return buf
}

func structUnpackTimerData(data []byte) (namespace, queue, jobID string, err error) {
	namespaceLen := binary.LittleEndian.Uint16(data)
	namespace = string(data[2 : namespaceLen+2])
	queueLen := binary.LittleEndian.Uint16(data[2+namespaceLen:])
	queue = string(data[2+namespaceLen+2 : 2+namespaceLen+2+queueLen])
	JobIDLen := binary.LittleEndian.Uint16(data[2+namespaceLen+2+queueLen:])
	jobID = string(data[2+namespaceLen+2+queueLen+2:])
	if len(jobID) != int(JobIDLen) {
		return "", "", "", errors.New("corrupted data")
	}
	return
}

func (t *Timer) Add(namespace, queue, jobID string, delaySecond uint32, tries uint16) error {
	metrics.timerAddJobs.WithLabelValues(t.redis.Name).Inc()
	timestamp := time.Now().Unix() + int64(delaySecond)

	score := encodeScore(timestamp, tries)
	member := structPackTimerData(namespace, queue, jobID)
	err := t.redis.Conn.ZAdd(dummyCtx, t.Name(), &redis.Z{Score: score, Member: member}).Err()
	if err != nil {
		return err
	}

	// We can ignore the error when removing job id from the backup queue
	// coz it harms nothing even respawn them into the timer set.
	_ = t.removeFromBackup(namespace, queue, jobID)
	return nil
}

func (t *Timer) BackupName() string {
	return strings.Join([]string{t.Name(), "backup"}, "/")
}

func (t *Timer) addToBackup(namespace, queue, jobID string, tries uint16) error {
	score := encodeScore(time.Now().Unix(), tries)
	member := structPackTimerData(namespace, queue, jobID)
	return t.redis.Conn.ZAdd(dummyCtx, t.BackupName(), &redis.Z{Score: score, Member: member}).Err()
}

func (t *Timer) removeFromBackup(namespace, queue, jobID string) error {
	member := structPackTimerData(namespace, queue, jobID)
	metrics.timerRemoveBackupJobs.WithLabelValues(t.redis.Name).Inc()
	return t.redis.Conn.ZRem(dummyCtx, t.BackupName(), member).Err()
}

// Tick pump all due jobs to the target queue
func (t *Timer) tick() {
	tick := time.NewTicker(t.interval)
	checkBackupTicker := time.NewTicker(t.checkBackupInterval)
	for {
		select {
		case now := <-tick.C:
			currentSecond := now.Unix()
			t.pump(currentSecond)
		case now := <-checkBackupTicker.C:
			t.pumpBackup(now.Unix())
		case <-t.shutdown:
			return
		}
	}
}

func (t *Timer) pumpBackup(currentSecond int64) {
	maxScore := encodeScore(currentSecond-int64(t.checkBackupInterval.Seconds()), math.MaxUint16)
	newScore := encodeScore(currentSecond, 0)
	val, err := t.redis.Conn.EvalSha(dummyCtx, t.pumpBackupSHA,
		[]string{t.Name(), QueuePrefix, PoolPrefix},
		newScore, maxScore, BatchSize,
	).Result()

	if err != nil {
		if isLuaScriptGone(err) { // when redis restart, the script needs to be uploaded again
			sha, err := t.redis.Conn.ScriptLoad(dummyCtx, luaPumpBackupScript).Result()
			if err != nil {
				logger.WithField("err", err).Error("Failed to reload script")
				time.Sleep(time.Second)
				return
			}
			t.pumpBackupSHA = sha
		}
		logger.WithField("err", err).Error("Failed to pump")

		val, err = t.redis.Conn.EvalSha(dummyCtx, t.pumpBackupSHA,
			[]string{t.Name(), QueuePrefix, PoolPrefix},
			newScore, maxScore, BatchSize,
		).Result()
		if err != nil {
			logger.WithField("err", err).Error("Failed to pump")
			return
		}
	}
	n, _ := val.(int64)
	if n > 0 {
		logger.WithField("count", n).Warn("Find lost jobs")
	}
}

func (t *Timer) pump(currentSecond int64) {
	maxScore := encodeScore(currentSecond, math.MaxUint16)
	for {
		val, err := t.redis.Conn.EvalSha(dummyCtx, t.pumpSHA,
			[]string{t.Name(), QueuePrefix, PoolPrefix, DeadLetterPrefix},
			maxScore, BatchSize,
		).Result()

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

func (t *Timer) BackupSize() (int64, error) {
	return t.redis.Conn.ZCard(dummyCtx, t.BackupName()).Result()
}
