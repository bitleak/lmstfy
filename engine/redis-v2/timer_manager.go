package redis_v2

import (
	"fmt"
	"time"

	"github.com/Jeffail/tunny"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

const (
	luaCalculateSequenceScript = `
local instances_set_key = KEYS[1]
local now = tonumber(ARGV[1])
local instances = redis.call("HGETALL", instances_set_key)
local result = {}
for i = 1, #instances, 2 do
	if now > tonumber(instances[i + 1]) then
		redis.call("HDEL", instances_set_key, instances[i])
	else
		table.insert(result, instances[i])
	end
end
table.sort(result)
return result
`
	luaPumpQueueScript = `
local delay_queue_prefix = KEYS[1]
local output_queue_prefix = KEYS[2]
local pool_prefix = KEYS[3]
local output_deadletter_prefix = KEYS[4]
local ns = ARGV[1]
local q = ARGV[2]
local now = ARGV[3]
local limit = ARGV[4]

local delay_queue = table.concat({delay_queue_prefix, ns, q}, "/")
local expiredMembers = redis.call("ZRANGEBYSCORE", delay_queue, 0, now, "LIMIT", 0, limit)

if #expiredMembers == 0 then
	return 0
end

for _,job_id in ipairs(expiredMembers) do
	local job_key = table.concat({pool_prefix, ns, q, job_id}, "/")
	if redis.call("EXISTS", job_key) > 0 then
		local tries = tonumber(redis.call("HGET", job_key, "tries"))
		-- only pump job to ready queue/dead letter if the job did not expire
		if tries == 0 then
			-- no more tries, move to dead letter
			redis.call("PERSIST", table.concat({pool_prefix, ns, q, job_id}, "/"))  -- remove ttl
			redis.call("LPUSH", table.concat({output_deadletter_prefix, ns, q}, "/"), job_id)
		elseif tries ~= nil then
			-- move to ready queue
			redis.call("LPUSH", table.concat({output_queue_prefix, ns, q}, "/"), job_id)
		end
	end
end
redis.call("ZREM", delay_queue, unpack(expiredMembers))
return #expiredMembers
`
)

const (
	TimerManagerInstanceSetKey = "_lmstfy_v2_timer_manager_set_"

	TimerManagerInstanceRegisterTTL   = 6
	TimerManagerInstanceCheckInterval = 2

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
	stop          chan bool

	_calculateSequenceLuaSha1 string
	_pumpQueueSHA             string
}

func NewTimerManager(queueManager *QueueManager, redis *RedisInstance) (*TimerManager, error) {
	id, err := genTimerManagerID()
	if err != nil {
		return nil, err
	}
	tm := &TimerManager{
		sequence:     -1,
		id:           id,
		queueManager: queueManager,
		redis:        redis,
		stop:         make(chan bool),
	}
	tm.pool = tunny.NewFunc(TimerManagerDefaultPoolSize, tm.pump)
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
	go tm.candidate()
	go tm.elect()
	go tm.startPump()
	return tm, nil
}

func (m *TimerManager) delayQueueName(namespace, queue string) string {
	return fmt.Sprintf("%s/%s/%s", DelayQueuePrefix, namespace, queue)
}

func (m *TimerManager) Add(namespace, queue, jobID string, delaySecond uint32) error {
	//metrics.timerAddJobs.WithLabelValues(t.redis.Name).Inc()
	timestamp := time.Now().Unix() + int64(delaySecond)
	return m.redis.Conn.ZAdd(dummyCtx, m.delayQueueName(namespace, queue), &redis.Z{Score: float64(timestamp), Member: []byte(jobID)}).Err()
}

// Register register timer manager to a redis set, key is time manager id, value is register expired time
func (m *TimerManager) register() error {
	_, err := m.redis.Conn.HSet(dummyCtx, TimerManagerInstanceSetKey, m.id, time.Now().Unix()+TimerManagerInstanceRegisterTTL).Result()
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
		return fmt.Errorf("invalid sequence calculation, result must be an slice")
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

func (m *TimerManager) pump(i interface{}) interface{} {
	q := i.(queue)
	for {
		val, err := m.redis.Conn.EvalSha(dummyCtx, m._pumpQueueSHA, []string{DelayQueuePrefix, ReadyQueuePrefix, PoolPrefix, DeadLetterPrefix}, q.namespace, q.queue, time.Now().Unix(), BatchSize).Result()
		if err != nil {
			if isLuaScriptGone(err) { // when redis restart, the script needs to be uploaded again
				err := m.preloadPumpQueueLuaScript()
				if err != nil {
					logger.WithField("err", err).Error("Failed to reload script")
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
		//metrics.timerDueJobs.WithLabelValues(t.redis.Name).Add(float64(n))
		if n == BatchSize {
			// There might have more expired jobs to pump
			//metrics.timerFullBatches.WithLabelValues(t.redis.Name).Inc()
			continue
		}
		return nil
	}
}

func (m *TimerManager) startPump() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			instanceCount := m.instanceCount
			seq := m.sequence
			if seq == TimerManagerNoRoleSequence {
				continue
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
		case <-m.stop:
			logger.Info("stop timer manager pump")
			return
		}
	}
}

func (m *TimerManager) candidate() {
	ticker := time.NewTicker(TimerManagerInstanceCheckInterval * time.Second)
	for {
		select {
		case <-ticker.C:
			err := m.register()
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":          err,
					"timerManagerID": m.id,
				}).Error("register timer manager error")
			}
		case <-m.stop:
			logger.Info("stop timer manager candidate")
			err := m.unregister()
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":          err,
					"timerManagerID": m.id,
				}).Error("unregister timer manager error")
			}
			return
		}
	}
}

func (m *TimerManager) elect() {
	ticker := time.NewTicker(TimerManagerInstanceCheckInterval * time.Second)
	for {
		select {
		case now := <-ticker.C:
			deadline := now.Unix()
			err := m.calculateSequence(deadline)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":          err,
					"timerManagerID": m.id,
					"deadline":       deadline,
				}).Error("unregister timer manager error")
			}
		case <-m.stop:
			logger.Info("stop timer manager elect")
			return
		}
	}
}

func (m *TimerManager) Close() {
	close(m.stop)
}
