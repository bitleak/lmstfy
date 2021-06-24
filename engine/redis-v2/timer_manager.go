package redis_v2

import (
	"fmt"
	"time"

	"github.com/Jeffail/tunny"
	"github.com/go-redis/redis/v8"
	"github.com/oif/gokit/wait"
	"github.com/sirupsen/logrus"
)

const (
	luaCalculateSequenceScript = `
local instancesSetKey = KEYS[1]
local now = tonumber(ARGV[1])
local instances = redis.call("HGETALL", instancesSetKey)
local result = {}
for i = 1, #instances, 2 do
	if now > tonumber(instances[i + 1]) then
		redis.call("HDEL", instancesSetKey, instances[i])
	else
		table.insert(result, instances[i])
	end
end
table.sort(result)
return result
`
	luaPumpQueueScript = `
local delayQueue = KEYS[1]
local readyQueue = KEYS[2]
local poolPrefix = KEYS[3]
local deadletter = KEYS[4]
local now = ARGV[1]
local limit = ARGV[2]

local expiredMembers = redis.call("ZRANGEBYSCORE", delayQueue, 0, now, "LIMIT", 0, limit)

if #expiredMembers == 0 then
	return 0
end

for _,jobID in ipairs(expiredMembers) do
	local jobKey = table.concat({poolPrefix, jobID}, "/")
	if redis.call("EXISTS", jobKey) > 0 then
		-- only pump job to ready queue/dead letter if the job did not expire
		local tries = tonumber(redis.call("HGET", jobKey, "tries"))
		if tries == 0 then
			-- no more tries, move to dead letter
			redis.call("PERSIST", jobKey)  -- remove ttl
			redis.call("LPUSH", deadletter, jobID)
		elseif tries ~= nil then
			-- move to ready queue
			redis.call("LPUSH", readyQueue, jobID)
		end
	end
end
redis.call("ZREM", delayQueue, unpack(expiredMembers))
return #expiredMembers
`
)

const (
	TimerManagerInstanceSetKey = "_lmstfy_v2_timer_manager_set_"

	TimerManagerInstanceRegisterTTL   = 6 * time.Second
	TimerManagerInstanceCheckInterval = 2 * time.Second

	TimerManagerNoRoleSequence = -1
	TimerManagerMasterSequence = 0

	TimerManagerPumpGoroutineRatio = 4
	TimerManagerDefaultPoolSize    = 10
)

type TimerManager struct {
	sequence      int
	instanceCount int
	pool          *tunny.Pool
	id            string
	queueManager  *QueueManager
	redis         *RedisInstance
	stop          chan struct{}

	_calculateSequenceLuaSha1 string
	_pumpQueueSHA             string
}

func NewTimerManager(queueManager *QueueManager, redis *RedisInstance) (*TimerManager, error) {
	id, err := genTimerManagerID()
	if err != nil {
		return nil, err
	}
	tm := &TimerManager{
		sequence:     TimerManagerNoRoleSequence,
		id:           id,
		queueManager: queueManager,
		redis:        redis,
		stop:         make(chan struct{}),
	}
	tm.pool = tunny.NewFunc(TimerManagerDefaultPoolSize, tm.pumpQueue)
	err = tm.preloadCalculateSequenceLuaScript()
	if err != nil {
		return nil, err
	}
	err = tm.preloadPumpQueueLuaScript()
	if err != nil {
		return nil, err
	}
	err = tm.register()
	if err != nil {
		return nil, err
	}
	err = tm.calculateSequence(time.Now().Unix())
	if err != nil {
		return nil, err
	}
	go wait.Keep(tm.heartbeat, TimerManagerInstanceCheckInterval, true, tm.stop)
	go wait.Keep(tm.elect, TimerManagerInstanceCheckInterval, true, tm.stop)
	go wait.Keep(tm.pump, time.Second, true, tm.stop)
	return tm, nil
}

func (m *TimerManager) delayQueueName(ns, q string) string {
	return (&queue{namespace: ns, queue: q}).DelayQueueString()
}

func (m *TimerManager) Add(namespace, queue, jobID string, delaySecond uint32) error {
	metrics.timerAddJobs.WithLabelValues(m.redis.Name).Inc()
	timestamp := time.Now().Unix() + int64(delaySecond)
	return m.redis.Conn.ZAdd(dummyCtx, m.delayQueueName(namespace, queue), &redis.Z{Score: float64(timestamp), Member: []byte(jobID)}).Err()
}

// Register register timer manager to a redis set, key is time manager id, value is register expired time
func (m *TimerManager) register() error {
	_, err := m.redis.Conn.HSet(dummyCtx, TimerManagerInstanceSetKey, m.id, time.Now().Add(TimerManagerInstanceRegisterTTL).Unix()).Result()
	return err
}

// Unregister remove timer manager from regis set
func (m *TimerManager) unregister() error {
	_, err := m.redis.Conn.HDel(dummyCtx, TimerManagerInstanceSetKey, m.id).Result()
	return err
}

func (m *TimerManager) preloadCalculateSequenceLuaScript() error {
	sha1, err := m.redis.Conn.ScriptLoad(dummyCtx, luaCalculateSequenceScript).Result()
	if err != nil {
		return err
	}
	m._calculateSequenceLuaSha1 = sha1
	return nil
}

func (m *TimerManager) preloadPumpQueueLuaScript() error {
	sha1, err := m.redis.Conn.ScriptLoad(dummyCtx, luaPumpQueueScript).Result()
	if err != nil {
		return err
	}
	m._pumpQueueSHA = sha1
	return nil
}

// CalculateSequence calculate timer manager sequence number by lua script, sequence will be -1 if this time manager not exist in lua result
func (m *TimerManager) calculateSequence(deadline int64) error {
	val, err := m.redis.Conn.EvalSha(dummyCtx, m._calculateSequenceLuaSha1, []string{TimerManagerInstanceSetKey}, deadline).Result()
	if err != nil {
		if !isLuaScriptGone(err) { // when redis restart, the script needs to be uploaded again
			return err
		}
		if err := m.preloadCalculateSequenceLuaScript(); err != nil {
			return err
		}
		val, err = m.redis.Conn.EvalSha(dummyCtx, m._calculateSequenceLuaSha1, []string{TimerManagerInstanceSetKey}, deadline).Result()
		if err != nil {
			return err
		}
	}
	instances, ok := val.([]interface{})
	if !ok {
		return fmt.Errorf("invalid sequence calculation, result must be a slice")
	}
	for seq, instance := range instances {
		if instance.(string) == m.id {
			m.sequence = seq
			m.instanceCount = len(instances)
			return nil
		}
	}
	m.sequence = -1
	return nil
}

func (m *TimerManager) pumpQueue(i interface{}) interface{} {
	q := i.(queue)
	for {
		val, err := m.redis.Conn.EvalSha(dummyCtx, m._pumpQueueSHA, []string{q.DelayQueueString(), q.ReadyQueueString(), q.PoolPrefixString(), q.DeadletterString()}, time.Now().Unix(), BatchSize).Result()
		if err != nil {
			if isLuaScriptGone(err) { // when redis restart, the script needs to be uploaded again
				err := m.preloadPumpQueueLuaScript()
				if err != nil {
					logger.WithError(err).Error("timer manager load pump lua script error")
					return err
				}
				continue
			}
			logger.WithFields(logrus.Fields{
				"error":     err,
				"namespace": q.namespace,
				"queue":     q.queue,
			}).Error("pump error")
			return err
		}
		n, _ := val.(int64)
		logger.WithField("count", n).Debug("Due jobs")
		metrics.timerDueJobs.WithLabelValues(m.redis.Name).Add(float64(n))
		if n == BatchSize {
			// There might have more expired jobs to pump
			metrics.timerFullBatches.WithLabelValues(m.redis.Name).Inc()
			continue
		}
		return nil
	}
}

func (m *TimerManager) pump() {
	instanceCount := m.instanceCount
	seq := m.sequence
	if seq == TimerManagerNoRoleSequence {
		return
	}
	queues := m.queueManager.Queues()
	queuesLen := len(queues)
	startIdx := 0
	step := 1
	poolSize := queuesLen / step / TimerManagerPumpGoroutineRatio

	if instanceCount > 2 && seq != TimerManagerMasterSequence {
		// if we have only two or less instances, every instance will be master.
		// otherwise, sequence 0 will be master, other instance divide queues equally
		startIdx = seq - 1
		step = instanceCount - 1
		poolSize = queuesLen / step / TimerManagerPumpGoroutineRatio
	}
	if poolSize > m.pool.GetSize() {
		m.pool.SetSize(poolSize)
	}
	for i := startIdx; i < queuesLen; i = i + step {
		m.pool.Process(queues[i])
	}
}

func (m *TimerManager) heartbeat() {
	if err := m.register(); err != nil {
		logger.WithFields(logrus.Fields{
			"error":          err,
			"timerManagerID": m.id,
		}).Error("timer manager heartbeat error")
	}
}

func (m *TimerManager) elect() {
	deadline := time.Now().Unix()
	if err := m.calculateSequence(deadline); err != nil {
		logger.WithFields(logrus.Fields{
			"error":          err,
			"timerManagerID": m.id,
			"deadline":       deadline,
		}).Error("timer manager elect error")
	}
}

func (m *TimerManager) Close() {
	if m.stop != nil {
		close(m.stop)
	}
	if err := m.unregister(); err != nil {
		logger.WithFields(logrus.Fields{
			"error":          err,
			"timerManagerID": m.id,
		}).Error("unregister timer manager error")
	}
}

type TimerSize struct {
	queue queue
	redis *RedisInstance
}

func NewTimerSize(ns, q string, redis *RedisInstance) *TimerSize {
	return &TimerSize{
		queue: queue{namespace: ns, queue: q},
		redis: redis,
	}
}

func (m *TimerSize) Size() (int64, error) {
	return m.redis.Conn.ZCard(dummyCtx, m.queue.DelayQueueString()).Result()
}
