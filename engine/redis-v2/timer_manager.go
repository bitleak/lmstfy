package redis_v2

import (
	"fmt"
	"time"

	"github.com/Jeffail/tunny"
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
)

const (
	TimerManagerInstanceSetKey        = "_lmstfy_v2_timer_manager_set_"
	TimerManagerInstanceRegisterTTL   = 6
	TimerManagerInstanceCheckInterval = 2

	TimerManagerNoRoleSequence = -1
	TimerManagerMasterSequence = 0
)

type TimerManager struct {
	sequence     int
	pool         tunny.Pool
	id           string
	queueManager *QueueManager
	redis        *RedisInstance
	stop         chan bool

	_calculateSequenceLuaSha1 string
}

func NewTimerManager(queueManager *QueueManager, redis *RedisInstance) (*TimerManager, error) {
	id, err := genTimerManagerID()
	if err != nil {
		return nil, err
	}
	tm := &TimerManager{
		sequence:     -1,
		pool:         tunny.Pool{},
		id:           id,
		queueManager: queueManager,
		redis:        redis,
		stop:         make(chan bool),
	}
	err = tm.preloadCalculateSequenceLuaScript()
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
	return tm, nil
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
			return nil
		}
	}
	m.sequence = -1
	return nil
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
