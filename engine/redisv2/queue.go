package redis

import (
	"errors"
	"fmt"
	"time"

	"github.com/bitleak/lmstfy/engine"
	go_redis "github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

const (
	MaxQueueLength       = 10000
	StreamReadCount      = 1
	StreamNoBlockingFlag = -1
	ConsumerGroup        = "lmstfy_group"
	ConsumerName         = "lmstfy_consumer"
	StreamMessageField   = "jobinfo"
)

type QueueName struct {
	Namespace string
	Queue     string
}

func (k *QueueName) String() string {
	return join(QueuePrefix, k.Namespace, k.Queue)
}

func (k *QueueName) Decode(str string) error {
	fmt.Printf("decode queue name from stream:%s", str)
	splits := splits(3, str)
	fmt.Printf("decode splits:%v", splits)
	if len(splits) != 3 || splits[0] != QueuePrefix {
		return errors.New("invalid format")
	}
	k.Namespace = splits[1]
	k.Queue = splits[2]
	return nil
}

// Queue is the "ready queue" that has all the jobs that can be consumed right now
type Queue struct {
	name     QueueName
	redis    *RedisInstance
	timerset string // timerset is a delayed queue which stores jobs that are due in the future

	destroySHA string
}

func NewQueue(namespace, queue string, redis *RedisInstance) *Queue {
	// create consumer group for queue
	redis.Conn.XGroupCreateMkStream(dummyCtx, join(QueuePrefix, namespace, queue), ConsumerGroup, "$")
	timerName := getTimersetKey(namespace, queue)
	return &Queue{
		name:     QueueName{Namespace: namespace, Queue: queue},
		redis:    redis,
		timerset: timerName,

		// NOTE: deadletter and queue are actually the same data structure, we could reuse the lua script
		// to empty the redis list (used as queue here). all we need to do is pass the queue name as the
		// deadletter name.
		destroySHA: deleteDeadletterSHA,
	}
}

func (q *Queue) Name() string {
	return q.name.String()
}

// PushInstantJob pushes an instant job into queue stream, the job data format: ["job","{tries}{job id}"]
func (q *Queue) PushInstantJob(j engine.Job, tries uint16) error {
	if tries == 0 {
		return nil
	}
	if j.Namespace() != q.name.Namespace || j.Queue() != q.name.Queue {
		// Wrong queue for the job
		return engine.ErrWrongQueue
	}
	metrics.queueDirectPushJobs.WithLabelValues(q.redis.Name).Inc()
	val := structPack(tries, j.ID())
	args := &go_redis.XAddArgs{
		Stream: q.Name(),
		MaxLen: MaxQueueLength,
		Values: []string{StreamMessageField, val},
	}
	val, err := q.redis.Conn.XAdd(dummyCtx, args).Result()
	if err == nil {
		// record stream id for future ack sake
		ok, err := q.redis.Conn.SetNX(dummyCtx, getJobStreamIDKey(j.ID()), val, time.Duration(j.TTL())*time.Second).Result()
		if err != nil || !ok {
			err = errors.New("failed to record job stream id")
			return err
		}
	}
	return err
}

// PushDelayedJob pushes a future due job into timer set with its due data, the job data format: {tries}{job id}
func (q *Queue) PushDelayedJob(namespace, queue, jobID string, delaySecond uint32, tries uint16) error {
	metrics.timerAddJobs.WithLabelValues(q.redis.Name).Inc()
	timestamp := time.Now().Unix() + int64(delaySecond)
	buf := constructDelayedJobContent(namespace, queue, jobID, tries)
	return q.redis.Conn.ZAdd(dummyCtx, q.timerset, &go_redis.Z{Score: float64(timestamp), Member: buf}).Err()
}

// Pop a job. If the tries > 0, add job to the "in-flight" timer with timestamp
// set to `TTR + now()`; Or we might just move the job to "dead-letter".
func (q *Queue) Poll(timeoutSecond, ttrSecond uint32) (jobID string, tries uint16, err error) {
	_, jobID, tries, err = PollQueues(q.redis, []QueueName{q.name}, timeoutSecond, ttrSecond)
	return jobID, tries, err
}

// Return number of the current in-queue jobs
func (q *Queue) Size() (size int64, err error) {
	return q.redis.Conn.LLen(dummyCtx, q.name.String()).Result()
}

// Peek a right-most element in the list without popping it
func (q *Queue) Peek() (jobID string, tries uint16, err error) {
	res, err := q.redis.Conn.XRevRangeN(dummyCtx, q.Name(), "+", "-", 1).Result()
	switch err {
	case nil:
		// continue
	case go_redis.Nil:
		return "", 0, engine.ErrNotFound
	default:
		return "", 0, err
	}
	val := res[0].Values[StreamMessageField].(string)
	tries, jobID, err = structUnpack(val)
	return jobID, tries, err
}

func (q *Queue) Destroy() (count int64, err error) {
	poolPrefix := PoolJobKeyPrefix(q.name.Namespace, q.name.Queue)
	var batchSize int64 = 100
	for {
		val, err := q.redis.Conn.EvalSha(dummyCtx, q.destroySHA, []string{q.Name(), poolPrefix}, batchSize).Result()
		if err != nil {
			if isLuaScriptGone(err) {
				if err := PreloadDeadLetterLuaScript(q.redis); err != nil {
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
func PollQueues(redis *RedisInstance, queueNames []QueueName, timeoutSecond, ttrSecond uint32) (queueName *QueueName, jobID string, retries uint16, err error) {
	defer func() {
		if jobID != "" {
			metrics.queuePopJobs.WithLabelValues(redis.Name).Inc()
		}
	}()

	// ex. streams args format: ["q1","q2",">",">"]
	keys := make([]string, 2*len(queueNames))
	for i := range keys {
		if i < len(queueNames) {
			keys[i] = queueNames[i].String()
			continue
		}
		keys[i] = ">"
	}
	val, results := make([]string, 2), make([]go_redis.XStream, 0)
	args := &go_redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: ConsumerName,
		Streams:  keys,
		Count:    StreamReadCount,
		Block:    StreamNoBlockingFlag,
	}
	// try none-blocking read first inorder to avoid receiving multi message from different streams
	results, err = redis.Conn.XReadGroup(dummyCtx, args).Result()
	if len(results) > 0 {
		fmt.Printf("xread noblock result:%v", results)
		msg := results[0].Messages[0]
		val[0], val[1] = results[0].Stream, msg.Values[StreamMessageField].(string)
	} else {
		// try blocking read when timeout second is specified
		if timeoutSecond > 0 {
			args.Block = time.Duration(timeoutSecond) * time.Second
			results, err = redis.Conn.XReadGroup(dummyCtx, args).Result()
			fmt.Printf("xread block result:%v", results)
			// in case of receiving multiple messages, will return the first one
			// and the rest which are now in pending state will be handled later
			if len(results) > 0 {
				msg := results[0].Messages[0]
				val[0], val[1] = results[0].Stream, msg.Values[StreamMessageField].(string)
			}
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
	if err := queueName.Decode(val[0]); err != nil {
		logger.WithField("err", err).Error("Failed to decode queue name")
		return nil, "", 0, err
	}
	tries, jobID, err := structUnpack(val[1])
	fmt.Printf("tries:%v; jobid:%v; err:%v", tries, jobID, err)
	if err != nil {
		logger.WithField("err", err).Error("Failed to unpack lua struct data")
		return nil, "", 0, err
	}

	if tries < 0 {
		logger.WithFields(logrus.Fields{
			"jobID": jobID,
			"ttr":   ttrSecond,
			"queue": queueName.String(),
		}).Error("Job with tries < 0 appeared")
		return nil, "", 0, fmt.Errorf("Job %s with tries < 0 appeared", jobID)
	}
	return queueName, jobID, tries, nil
}
