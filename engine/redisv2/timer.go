package redis

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
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
	local val = struct.pack("HHc0", tonumber(tries), #job_id, job_id)
	redis.call("XADD", table.concat({output_queue_prefix, ns, q}, "/"), "*", stream_field_name, val)	
end
redis.call("ZREM", zset_key, unpack(expiredMembers))
return #expiredMembers
`
)

const (
	MinMsgIdleTime       = 10 * time.Second
	RenewStreamIDExpTime = 3 * time.Minute
	DefaultPendingSize   = 50
)

// Timer manages a set of delay queues. Periodically, timer will check all delay queues
// and push due jobs into ready queue.
type Timer struct {
	name        string
	redisCli    *RedisInstance
	interval    time.Duration
	shutdown    chan struct{}
	members     map[string]struct{}
	rwmu        sync.RWMutex
	recycleTime time.Duration
	pumpSHA     string
}

// NewTimer return an instance of delay queue
func NewTimer(name string, redis *RedisInstance, interval, recycleTime time.Duration) (*Timer, error) {
	timer := &Timer{
		name:        name,
		redisCli:    redis,
		interval:    interval,
		recycleTime: recycleTime,
		shutdown:    make(chan struct{}),
		members:     make(map[string]struct{}),
	}

	sha, err := redis.Conn.ScriptLoad(dummyCtx, luaPumpQueueScript).Result()
	if err != nil {
		logger.WithField("err", err).Error("Failed to preload lua script in timer")
		return nil, err
	}
	timer.pumpSHA = sha

	go timer.Tick()
	go timer.ProcessPendingMsg()
	return timer, nil
}

func (t *Timer) Name() string {
	return t.name
}

// Tick pump all due jobs to the target queue
func (t *Timer) Tick() {
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
	t.rwmu.RLock()
	defer t.rwmu.RUnlock()
	for {
		for member := range t.members {
			val, err := t.redisCli.Conn.EvalSha(dummyCtx, t.pumpSHA,
				[]string{member, QueuePrefix, PoolPrefix, StreamMessageField}, currentSecond, BatchSize).Result()
			if err != nil {
				if isLuaScriptGone(err) { // when redis restart, the script needs to be uploaded again
					sha, err := t.redisCli.Conn.ScriptLoad(dummyCtx, luaPumpQueueScript).Result()
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
				metrics.timerFullBatches.WithLabelValues(t.redisCli.Name).Inc()
				time.Sleep(10 * time.Millisecond) // Hurry up! accelerate pumping the due jobs
				continue
			}
		}
		logger.WithField("count", count).Debug("Due jobs")
		metrics.timerDueJobs.WithLabelValues(t.redisCli.Name).Add(float64(count))
		return
	}
}

func (t *Timer) Shutdown() {
	close(t.shutdown)
}

func (t *Timer) Size() (size int64, err error) {
	return t.redisCli.Conn.ZCard(dummyCtx, t.name).Result()
}

// AddTimerSet adds a new timer set to Timer
func (t *Timer) AddTimerSet(name string) {
	t.rwmu.Lock()
	t.members[name] = struct{}{}
	t.rwmu.Unlock()
}

// ProcessPendingMsg periodically checks pending messages for all streams and
// either put them back to ready state or simply discard them based on ttr and idle time
func (t *Timer) ProcessPendingMsg() {
	tick := time.NewTicker(t.recycleTime)
	for {
		select {
		case <-tick.C:
			t.recycle()
		case <-t.shutdown:
			return
		}
	}
}

func (t *Timer) recycle() {
	fmt.Println("enter recycle")
	fmt.Println("members", t.members)
	t.rwmu.RLock()
	for member := range t.members {
		fmt.Println("process member", member)

		vals := splits(3, member)
		if len(vals) != 3 || vals[0] != TimerSetPrefix {
			continue
		}
		namespace, queue := vals[1], vals[2]
		stream := join(QueuePrefix, namespace, queue)
		err := handlePendingMsg(t.redisCli, stream, namespace, queue)
		if err != nil {
			continue
		}
	}
	t.rwmu.RUnlock()
}

func handlePendingMsg(client *RedisInstance, stream, namespace, queue string) error {
	extArgs := &redis.XPendingExtArgs{
		Stream: stream,
		Group:  ConsumerGroup,
		Idle:   MinMsgIdleTime,
		Start:  "-",
		End:    "+",
		Count:  DefaultPendingSize,
	}
	msgs, err := client.Conn.XPendingExt(dummyCtx, extArgs).Result()
	fmt.Printf("get pending msg: %v and error: %v", msgs, err)
	if err != nil {
		return err
	}
	// check each job's tries. If tries <= 0 then move it to dead letter, else renew the job
	for _, msg := range msgs {
		tries, jobID := getJobInfo(client, stream, msg.ID)
		fmt.Println("get msg tries", tries)
		if tries <= 0 {
			val := structPack(1, jobID)
			err := client.Conn.Persist(dummyCtx, PoolJobKey2(namespace, queue, jobID)).Err()
			if err != nil {
				continue
			}
			client.Conn.LPush(dummyCtx, join(DeadLetterPrefix, namespace, queue), val)
			// remove the original msg out of pending stream
			client.Conn.XAck(dummyCtx, queue, ConsumerGroup, msg.ID)
			continue
		}
		// write job back to stream with updated number of tries
		renewJob(client, tries-1, stream, jobID, msg.ID)
	}
	return nil
}

func getJobInfo(client *RedisInstance, stream, id string) (uint16, string) {
	res, err := client.Conn.XRangeN(dummyCtx, stream, id, id, 1).Result()
	if err != nil {
		return 0, ""
	}
	val := res[0].Values[StreamMessageField].(string)
	tries, jobID, err := structUnpack(val)
	if err != nil {
		return 0, ""
	}
	return tries, jobID
}

func renewJob(client *RedisInstance, tries uint16, stream, jobID, msgID string) {
	val := structPack(tries, jobID)
	args := &redis.XAddArgs{
		Stream: stream,
		MaxLen: MaxQueueLength,
		Values: []string{StreamMessageField, val},
	}
	streamID, err := client.Conn.XAdd(dummyCtx, args).Result()
	fmt.Printf("renew job: %v, new stream id: %v, error:%v", jobID, streamID, err)
	if err == nil {
		// record stream id for future ack sake
		client.Conn.Set(dummyCtx, getJobStreamIDKey(jobID), streamID, RenewStreamIDExpTime)
		fmt.Println("set stream id ok")
	}
	// remove the original msg out of pending stream
	res, err := client.Conn.XAck(dummyCtx, stream, ConsumerGroup, msgID).Result()
	fmt.Printf("ack old pending stream id %v, and res:%v, error: %v", msgID, res, err)
}
