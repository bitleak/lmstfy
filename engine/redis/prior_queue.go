package redis

import (
	"errors"
	"fmt"
	"time"

	"github.com/bitleak/lmstfy/engine"
	go_redis "github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
)

// PriorQueue is the "ready queue" that has all the jobs that can be consumed right now
type PriorQueue struct {
	name  QueueName
	redis *RedisInstance
	timer *Timer

	luaDestroySHA string
}

func NewPriorQueue(namespace, queue string, redis *RedisInstance, timer *Timer) *PriorQueue {
	return &PriorQueue{
		name:  QueueName{Namespace: namespace, Queue: queue},
		redis: redis,
		timer: timer,

		// NOTE: deadletter and queue are actually the same data structure, we could reuse the lua script
		// to empty the redis list (used as queue here). all we need to do is pass the queue name as the
		// deadletter name.
		luaDestroySHA: _luaDeletePriorQueueSHA,
	}
}

func (q *PriorQueue) Name() string {
	return q.name.String()
}

// Push a job into the queue, the job data format: {tries}{job id}
func (q *PriorQueue) Push(j engine.Job, tries uint16) error {
	if tries == 0 {
		return nil
	}
	if j.Namespace() != q.name.Namespace || j.Queue() != q.name.Queue {
		// Wrong queue for the job
		return engine.ErrWrongQueue
	}
	metrics.queueDirectPushJobs.WithLabelValues(q.redis.Name).Inc()
	val := structPack(tries, j.ID())
	return q.redis.Conn.ZAdd(q.Name(), go_redis.Z{Score: float64(j.Priority()), Member: val}).Err()
}

// Pop a job. If the tries > 0, add job to the "in-flight" timer with timestamp
// set to `TTR + now()`; Or we might just move the job to "dead-letter".
func (q *PriorQueue) Poll(timeoutSecond, ttrSecond uint32) (jobID string, tries uint16, err error) {
	_, jobID, tries, err = PollPriorQueues(q.redis, q.timer, []QueueName{q.name}, timeoutSecond, ttrSecond, false)
	return jobID, tries, err
}

// PollWithFrozenTries was same as `Poll` except would not consume tries
func (q *PriorQueue) PollWithFrozenTries(timeoutSecond, ttrSecond uint32) (jobID string, tries uint16, err error) {
	_, jobID, tries, err = PollPriorQueues(q.redis, q.timer, []QueueName{q.name}, timeoutSecond, ttrSecond, true)
	return jobID, tries, err
}

// Return number of the current in-queue jobs
func (q *PriorQueue) Size() (size int64, err error) {
	return q.redis.Conn.ZCard(q.name.String()).Result()
}

// Peek a right-most element in the list without popping it
func (q *PriorQueue) Peek() (jobID string, tries uint16, err error) {
	val, err := q.redis.Conn.ZRevRange(q.Name(), 0, 0).Result()
	switch err {
	case nil:
		// continue
	case go_redis.Nil:
		return "", 0, engine.ErrNotFound
	default:
		return "", 0, err
	}
	if len(val) == 0 {
		return "", 0, engine.ErrNotFound
	}
	tries, jobID, err = structUnpack(val[0])
	return jobID, tries, err
}

func (q *PriorQueue) Destroy() (count int64, err error) {
	poolPrefix := PoolJobKeyPrefix(q.name.Namespace, q.name.Queue)
	var batchSize int64 = 100
	for {
		val, err := q.redis.Conn.EvalSha(q.luaDestroySHA, []string{q.Name(), poolPrefix}, batchSize).Result()
		if err != nil {
			if isLuaScriptGone(err) {
				if err := PreloadDeadLetterLuaScript(q.redis, false); err != nil {
					logger.WithField("err", err).Error("Failed to load deadletter lua script")
				}
				continue
			}
			return count, err
		}
		n, _ := val.(int64)
		count += n
		if n < batchSize { // Dead letter is empty
			break
		}
	}
	return count, nil
}

// Poll from multiple queues using blocking method; OR pop a job from one queue using non-blocking method
func PollPriorQueues(redis *RedisInstance, timer *Timer, queueNames []QueueName, timeoutSecond, ttrSecond uint32, freezeTries bool) (queueName *QueueName, jobID string, retries uint16, err error) {
	defer func() {
		if jobID != "" {
			metrics.queuePopJobs.WithLabelValues(redis.Name).Inc()
		}
	}()
	var val go_redis.ZWithKey
	if timeoutSecond > 0 { // Blocking poll
		keys := make([]string, len(queueNames))
		for i, k := range queueNames {
			keys[i] = k.String()
		}
		val, err = redis.Conn.BZPopMax(time.Duration(timeoutSecond)*time.Second, keys...).Result()
	} else { // Non-Blocking fetch
		if len(queueNames) != 1 {
			return nil, "", 0, errors.New("non-blocking pop can NOT support multiple keys")
		}
		var result []go_redis.Z
		result, err = redis.Conn.ZPopMax(queueNames[0].String()).Result()
		if len(result) > 0 {
			val.Z = result[0]
		}
	}
	switch err {
	case nil:
		// continue
	case go_redis.Nil:
		logger.Debug("Job not found")
		return nil, "", 0, nil
	default:
		logger.WithField("err", err).Error("Failed to pop job from queue")
		return nil, "", 0, err
	}
	queueName = &QueueName{}
	if err := queueName.Decode(val.Key); err != nil {
		logger.WithField("err", err).Error("Failed to decode queue name")
		return nil, "", 0, err
	}
	if _, ok := val.Z.Member.(string); !ok {
		return nil, "", 0, errors.New("invalid job format")
	}
	tries, jobID, err := structUnpack(val.Z.Member.(string))
	if err != nil {
		logger.WithField("err", err).Error("Failed to unpack lua struct data")
		return nil, "", 0, err
	}

	if tries == 0 {
		logger.WithFields(logrus.Fields{
			"jobID": jobID,
			"ttr":   ttrSecond,
			"queue": queueName.String(),
		}).Error("Job with tries == 0 appeared")
		return nil, "", 0, fmt.Errorf("job %s with tries == 0 appeared", jobID)
	}
	if !freezeTries {
		tries = tries - 1
	}
	err = timer.Add(queueName.Namespace, queueName.Queue, jobID, ttrSecond, tries) // NOTE: tries is not decreased
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":   err,
			"jobID": jobID,
			"ttr":   ttrSecond,
			"queue": queueName.String(),
		}).Error("Failed to add job to timer for ttr")
		return queueName, jobID, tries, err
	}
	return queueName, jobID, tries, nil
}
