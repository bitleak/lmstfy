package redis

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/bitleak/lmstfy/engine"
	go_redis "github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

const (
	MaxQueueLength     = 10000
	StreamReadCount    = 1
	ConsumerGroup      = "lmstfy_group"
	ConsumerName       = "lmstfy_consumer"
	StreamMessageField = "jobinfo"
)

type QueueName struct {
	Namespace string
	Queue     string
}

func (k *QueueName) String() string {
	return join(QueuePrefix, k.Namespace, k.Queue)
}

func (k *QueueName) Decode(str string) error {
	splits := splits(3, str)
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
	timerName := GetTimersetKey(namespace, queue)
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
		ok, err := q.redis.Conn.SetNX(dummyCtx, GetJobStreamIDKey(j.ID()), val, time.Duration(j.TTL())*time.Second).Result()
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
	buf := ConstructDelayedJobContent(namespace, queue, jobID, tries)
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
	val, err := q.redis.Conn.LIndex(dummyCtx, q.Name(), -1).Result()
	switch err {
	case nil:
		// continue
	case go_redis.Nil:
		return "", 0, engine.ErrNotFound
	default:
		return "", 0, err
	}
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

	keys := make([]string, len(queueNames))
	for i, k := range queueNames {
		keys[i] = k.String()
	}
	val, results := make([]string, 2), make([]go_redis.XStream, 0)
	args := &go_redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: ConsumerName,
		Streams:  keys,
		Count:    StreamReadCount,
	}
	if timeoutSecond > 0 {
		args.Block = time.Duration(timeoutSecond) * time.Second
	} else {
		args.Block = -1
	}
	results, err = redis.Conn.XReadGroup(dummyCtx, args).Result()
	if len(results) > 1 {
		// todo receive multiple messages, will return the first one and write the rest back to stream
	}
	if len(results) > 0 {
		msg := results[0].Messages[0]
		val[0], val[1] = results[0].Stream, msg.Values[StreamMessageField].(string)
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
		return nil, "", 0, fmt.Errorf("Job %s with tries == 0 appeared", jobID)
	}
	return queueName, jobID, tries, nil
}

// Pack (tries, jobID) into lua struct pack of format "HHHc0", in lua this can be done:
//   ```local data = struct.pack("HHc0", tries, #job_id, job_id)```
func structPack(tries uint16, jobID string) (data string) {
	buf := make([]byte, 2+2+len(jobID))
	binary.LittleEndian.PutUint16(buf[0:], tries)
	binary.LittleEndian.PutUint16(buf[2:], uint16(len(jobID)))
	copy(buf[4:], jobID)
	return string(buf)
}

// Unpack the "HHc0" lua struct format, in lua this can be done:
//   ```local tries, job_id = struct.unpack("HHc0", data)```
func structUnpack(data string) (tries uint16, jobID string, err error) {
	buf := []byte(data)
	h1 := binary.LittleEndian.Uint16(buf[0:])
	h2 := binary.LittleEndian.Uint16(buf[2:])
	jobID = string(buf[4:])
	tries = h1
	if len(jobID) != int(h2) {
		err = errors.New("corrupted data")
	}
	return
}

// ConstructDelayedJobContent struct-pack the data in the format `Hc0Hc0HHc0`:
// {namespace len}{namespace}{queue len}{queue}{tries}{jobID len}{jobID}
// length are 2-byte uint16 in little-endian
func ConstructDelayedJobContent(namespace, queue, jobID string, tries uint16) []byte {
	namespaceLen := len(namespace)
	queueLen := len(queue)
	jobIDLen := len(jobID)
	buf := make([]byte, 2+namespaceLen+2+queueLen+2+2+jobIDLen)
	binary.LittleEndian.PutUint16(buf[0:], uint16(namespaceLen))
	copy(buf[2:], namespace)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen:], uint16(queueLen))
	copy(buf[2+namespaceLen+2:], queue)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen+2+queueLen:], tries)
	binary.LittleEndian.PutUint16(buf[2+namespaceLen+2+queueLen+2:], uint16(jobIDLen))
	copy(buf[2+namespaceLen+2+queueLen+2+2:], jobID)
	return buf
}

// GetJobStreamIDKey returns the stream id of a job
func GetJobStreamIDKey(jobID string) string {
	return join(StreamIDPrefix, jobID)
}

// GetQueueStreamName returns the stream name of a queue
func GetQueueStreamName(namespace, queue string) string {
	return join(QueuePrefix, namespace, queue)
}

// GetTimersetKey returns the timer set key of a queue
func GetTimersetKey(namespace, queue string) string {
	return join(TimerSetPrefix, namespace, queue)
}
