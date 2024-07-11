package client

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"
)

type Job struct {
	Namespace   string `json:"namespace"`
	Queue       string `json:"queue"`
	Data        []byte `json:"data"`
	ID          string `json:"job_id"`
	TTL         int64  `json:"ttl"`
	ElapsedMS   int64  `json:"elapsed_ms"`
	RemainTries int64  `json:"remain_tries"`
}

type LmstfyClient struct {
	scheme   string
	endpoint string

	Namespace string
	Token     string
	Host      string
	Port      int

	retry         int // retry when Publish failed (only some situations are worth of retrying)
	backOff       int // millisecond
	httpCli       *http.Client
	errorOnNilJob bool // return error when job is nil
}

func NewLmstfyClient(host string, port int, namespace, token string) *LmstfyClient {
	cli := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        128,
			MaxIdleConnsPerHost: 32,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: time.Minute,
			}).DialContext,
		},
		Timeout: maxReadTimeout * time.Second,
	}
	return NewLmstfyWithClient(cli, host, port, namespace, token)
}

// NewLmstfyWithClient allow using user defined http client to setup the lmstfy client
func NewLmstfyWithClient(cli *http.Client, host string, port int, namespace, token string) *LmstfyClient {
	scheme := "http"
	if url, err := url.Parse(host); err == nil && url.Scheme != "" {
		scheme = url.Scheme
		host = url.Host
	}

	return &LmstfyClient{
		Namespace: namespace,
		Token:     token,
		Host:      host,
		Port:      port,

		scheme:   scheme,
		endpoint: fmt.Sprintf("%s:%d", host, port),
		httpCli:  cli,
	}
}

// EnableErrorOnNilJob would make the client return error when
// the job was nil(maybe queue was not found)
func (c *LmstfyClient) EnableErrorOnNilJob() {
	c.errorOnNilJob = true
}

func (c *LmstfyClient) ConfigRetry(retryCount int, backOffMillisecond int) {
	c.retry = retryCount
	c.backOff = backOffMillisecond
}

// Publish a new job to the queue.
//   - ttlSecond is the time-to-live of the job. If it's zero, job won't expire; if it's positive, the value is the TTL.
//   - tries is the maximum times the job can be fetched.
//   - delaySecond is the duration before the job is released for consuming. When it's zero, no delay is applied.
func (c *LmstfyClient) Publish(queue string, data []byte, ttlSecond uint32, tries uint16, delaySecond uint32) (jobID string, e error) {
	return c.publish(nil, queue, "", data, ttlSecond, tries, delaySecond)
}

// RePublish delete(ack) the job of the queue and publish the job again.
//   - ttlSecond is the time-to-live of the job. If it's zero, job won't expire; if it's positive, the value is the TTL.
//   - tries is the maximum times the job can be fetched.
//   - delaySecond is the duration before the job is released for consuming. When it's zero, no delay is applied.
func (c *LmstfyClient) RePublish(job *Job, ttlSecond uint32, tries uint16, delaySecond uint32) (jobID string, e error) {
	return c.publish(nil, job.Queue, job.ID, job.Data, ttlSecond, tries, delaySecond)
}

// BatchPublish publish lots of jobs at one time
//   - ttlSecond is the time-to-live of the job. If it's zero, job won't expire; if it's positive, the value is the TTL.
//   - tries is the maximum times the job can be fetched.
//   - delaySecond is the duration before the job is released for consuming. When it's zero, no delay is applied.
func (c *LmstfyClient) BatchPublish(queue string, jobs []interface{}, ttlSecond uint32, tries uint16, delaySecond uint32) (jobIDs []string, e error) {
	return c.batchPublish(nil, queue, jobs, ttlSecond, tries, delaySecond)
}

// Consume a job. Consuming will decrease the job's tries by 1 first.
//   - ttrSecond is the time-to-run of the job. If the job is not finished before the TTR expires,
//     the job will be released for consuming again if the `(tries - 1) > 0`.
//   - timeoutSecond is the long-polling wait time. If it's zero, this method will return immediately
//     with or without a job; if it's positive, this method would polling for new job until timeout.
func (c *LmstfyClient) Consume(queue string, ttrSecond, timeoutSecond uint32) (job *Job, e error) {
	return c.consume(nil, queue, ttrSecond, timeoutSecond, false)
}

// ConsumeWithFreezeTries a job. Consuming with retries will not decrease the job's tries.
//   - ttrSecond is the time-to-run of the job. If the job is not finished before the TTR expires,
//     the job will be released for consuming again if the `(tries - 1) > 0`.
//   - timeoutSecond is the long-polling wait time. If it's zero, this method will return immediately
//     with or without a job; if it's positive, this method would polling for new job until timeout.
func (c *LmstfyClient) ConsumeWithFreezeTries(queue string, ttrSecond, timeoutSecond uint32) (job *Job, e error) {
	return c.consume(nil, queue, ttrSecond, timeoutSecond, true)
}

// BatchConsume consume some jobs. Consuming will decrease these jobs tries by 1 first.
//   - ttrSecond is the time-to-run of these jobs. If these jobs are not finished before the TTR expires,
//     these jobs will be released for consuming again if the `(tries - 1) > 0`.
//   - count is the job count of this consume. If it's zero or over 100, this method will return an error.
//     If it's positive, this method would return some jobs, and it's count is between 0 and count.
func (c *LmstfyClient) BatchConsume(queues []string, count, ttrSecond, timeoutSecond uint32) (jobs []*Job, e error) {
	return c.batchConsume(nil, queues, count, ttrSecond, timeoutSecond, false)
}

// BatchConsume consume some jobs. Consuming with freeze tries will not decrease these jobs tries.
//   - ttrSecond is the time-to-run of these jobs. If these jobs are not finished before the TTR expires,
//     these job will be released for consuming again if the `(tries - 1) > 0`.
//   - count is the job count of this consume. If it's zero or over 100, this method will return an error.
//     If it's positive, this method would return some jobs, and it's count is between 0 and count.
func (c *LmstfyClient) BatchConsumeWithFreezeTries(queues []string, count, ttrSecond, timeoutSecond uint32) (jobs []*Job, e error) {
	return c.batchConsume(nil, queues, count, ttrSecond, timeoutSecond, true)
}

// ConsumeFromQueues consumes from multiple queues with priority.
// The order of the queues in the params implies the priority. eg.
//   ConsumeFromQueues(120, 5, "queue-a", "queue-b", "queue-c")
// if all the queues have jobs to be fetched, the job in `queue-a` will be return.
func (c *LmstfyClient) ConsumeFromQueues(ttrSecond, timeoutSecond uint32, queues ...string) (job *Job, e error) {
	return c.consumeFromQueues(nil, ttrSecond, timeoutSecond, false, queues...)
}

// ConsumeFromQueuesWithFreezeTries consumes from multiple queues with priority.
func (c *LmstfyClient) ConsumeFromQueuesWithFreezeTries(ttrSecond, timeoutSecond uint32, queues ...string) (job *Job, e error) {
	return c.consumeFromQueues(nil, ttrSecond, timeoutSecond, true, queues...)
}

// Ack Marks a job as finished, so it won't be retried by others.
func (c *LmstfyClient) Ack(queue, jobID string) *APIError {
	return c.ack(nil, queue, jobID)
}

// QueueSize Get queue size. It means how many jobs are waiting in the queue for consuming.
func (c *LmstfyClient) QueueSize(queue string) (int, *APIError) {
	return c.queueSize(nil, queue)
}

// PeekQueue Peeks the job in the head of the queue
func (c *LmstfyClient) PeekQueue(queue string) (job *Job, e *APIError) {
	return c.peekQueue(nil, queue)
}

// PeekJob Peeks a specific job data
func (c *LmstfyClient) PeekJob(queue, jobID string) (job *Job, e *APIError) {
	return c.peekJob(nil, queue, jobID)
}

// PeekDeadLetter Peeks the dead letter of the queue
func (c *LmstfyClient) PeekDeadLetter(queue string) (deadLetterSize int, deadLetterHead string, e *APIError) {
	return c.peekDeadLetter(nil, queue)
}

// RespawnDeadLetter respawns the jobs of the given queue's dead letter
func (c *LmstfyClient) RespawnDeadLetter(queue string, limit, ttlSecond int64) (count int, e *APIError) {
	return c.respawnDeadLetter(nil, queue, limit, ttlSecond)
}

// DeleteDeadLetter deletes the given queue's dead letter
func (c *LmstfyClient) DeleteDeadLetter(queue string, limit int64) *APIError {
	return c.deleteDeadLetter(nil, queue, limit)
}

// <---------------------------------- THE CONTEXT VERSIONS OF THE METHODS ARE BELOW ---------------------------------->

// PublishWithContext a context version of Publish
func (c *LmstfyClient) PublishWithContext(ctx context.Context, queue string, data []byte, ttlSecond uint32, tries uint16,
	delaySecond uint32) (jobID string, err error) {
	return c.publish(nil, queue, "", data, ttlSecond, tries, delaySecond)
}

// RePublishWithContext a context version of RePublish
func (c *LmstfyClient) RePublishWithContext(ctx context.Context, job *Job, ttlSecond uint32, tries uint16,
	delaySecond uint32) (jobID string, err error) {
	return c.publish(ctx, job.Queue, job.ID, job.Data, ttlSecond, tries, delaySecond)
}

// BatchPublishWithContext a context version of BatchPublish
func (c *LmstfyClient) BatchPublishWithContext(ctx context.Context, queue string, jobDataSet []interface{},
	ttlSecond uint32, tries uint16, delaySecond uint32) (jobIDs []string, err error) {
	return c.batchPublish(ctx, queue, jobDataSet, ttlSecond, tries, delaySecond)
}

// ConsumeWithContext a context version of Consume
func (c *LmstfyClient) ConsumeWithContext(ctx context.Context, queue string, ttrSecond, timeoutSecond uint32) (*Job, error) {
	return c.consume(ctx, queue, ttrSecond, timeoutSecond, false)
}

// ConsumeWithFreezeTriesWithContext a context version of ConsumeWithFreezeTries
func (c *LmstfyClient) ConsumeWithFreezeTriesWithContext(ctx context.Context, queue string, ttrSecond, timeoutSecond uint32) (*Job, error) {
	return c.consume(ctx, queue, ttrSecond, timeoutSecond, true)
}

// BatchConsumeWithContext a context version of BatchConsume
func (c *LmstfyClient) BatchConsumeWithContext(ctx context.Context, queues []string, count, ttrSecond, timeoutSecond uint32) ([]*Job, error) {
	return c.batchConsume(ctx, queues, count, ttrSecond, timeoutSecond, false)
}

// BatchConsumeWithFreezeTriesWithContext a context version of BatchConsumeWithFreezeTries
func (c *LmstfyClient) BatchConsumeWithFreezeTriesWithContext(ctx context.Context, queues []string,
	count, ttrSecond, timeoutSecond uint32) ([]*Job, error) {
	return c.batchConsume(ctx, queues, count, ttrSecond, timeoutSecond, true)
}

// ConsumeFromQueuesWithContext a context version of ConsumeFromQueues
func (c *LmstfyClient) ConsumeFromQueuesWithContext(ctx context.Context, ttrSecond, timeoutSecond uint32,
	queues ...string) (*Job, error) {
	return c.consumeFromQueues(ctx, ttrSecond, timeoutSecond, false, queues...)
}

// ConsumeFromQueuesWithFreezeTriesWithContext a context version of ConsumeFromQueuesWithFreezeTries
func (c *LmstfyClient) ConsumeFromQueuesWithFreezeTriesWithContext(ctx context.Context, ttrSecond, timeoutSecond uint32,
	queues ...string) (*Job, error) {
	return c.consumeFromQueues(ctx, ttrSecond, timeoutSecond, true, queues...)
}

// AckWithContext a context version of Ack
func (c *LmstfyClient) AckWithContext(ctx context.Context, queue, jobID string) *APIError {
	return c.ack(ctx, queue, jobID)
}

// QueueSizeWithContext a context version of QueueSize
func (c *LmstfyClient) QueueSizeWithContext(ctx context.Context, queue string) (int, *APIError) {
	return c.queueSize(ctx, queue)
}

// PeekQueueWithContext a context version of PeekQueue
func (c *LmstfyClient) PeekQueueWithContext(ctx context.Context, queue string) (*Job, *APIError) {
	return c.peekQueue(ctx, queue)
}

// PeekJobWithContext a context version of PeekJob
func (c *LmstfyClient) PeekJobWithContext(ctx context.Context, queue, jobID string) (*Job, *APIError) {
	return c.peekJob(ctx, queue, jobID)
}

// PeekDeadLetterWithContext a context version of PeekDeadLetter
func (c *LmstfyClient) PeekDeadLetterWithContext(ctx context.Context, queue string) (int, string, *APIError) {
	return c.peekDeadLetter(ctx, queue)
}

// RespawnDeadLetterWithContext a context version of RespawnDeadLetter
func (c *LmstfyClient) RespawnDeadLetterWithContext(ctx context.Context, queue string, limit, ttlSecond int64) (int, *APIError) {
	return c.respawnDeadLetter(ctx, queue, limit, ttlSecond)
}

// DeleteDeadLetterWithContext a context version of DeleteDeadLetter
func (c *LmstfyClient) DeleteDeadLetterWithContext(ctx context.Context, queue string, limit int64) *APIError {
	return c.deleteDeadLetter(ctx, queue, limit)
}
