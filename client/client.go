package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
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

// Peek a specific job data
func (c *LmstfyClient) PeekJob(queue, jobID string) (job *Job, e *APIError) {
	req, err := c.getReq(http.MethodGet, path.Join(queue, "job", jobID), nil, nil)
	if err != nil {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
	resp, err := c.httpCli.Do(req)
	if err != nil {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		discardResponseBody(resp.Body)
		return nil, nil
	case http.StatusOK:
		// continue
	default:
		return nil, &APIError{
			Type:      ResponseErr,
			Reason:    parseResponseError(resp),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	job = &Job{}
	err = json.Unmarshal(respBytes, job)
	if err != nil {
		return nil, &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	return job, nil
}

// Peek the deadletter of the queue
func (c *LmstfyClient) PeekDeadLetter(queue string) (deadLetterSize int, deadLetterHead string, e *APIError) {
	req, err := c.getReq(http.MethodGet, path.Join(queue, "deadletter"), nil, nil)
	if err != nil {
		return 0, "", &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
	resp, err := c.httpCli.Do(req)
	if err != nil {
		return 0, "", &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, "", &APIError{
			Type:      ResponseErr,
			Reason:    parseResponseError(resp),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, "", &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	var respData struct {
		Namespace      string `json:"namespace"`
		Queue          string `json:"queue"`
		DeadLetterSize int    `json:"deadletter_size"`
		DeadLetterHead string `json:"deadletter_head"`
	}
	err = json.Unmarshal(respBytes, &respData)
	if err != nil {
		return 0, "", &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	return respData.DeadLetterSize, respData.DeadLetterHead, nil
}

func (c *LmstfyClient) RespawnDeadLetter(queue string, limit, ttlSecond int64) (count int, e *APIError) {
	if limit <= 0 {
		return 0, &APIError{
			Type:   RequestErr,
			Reason: "limit should be > 0",
		}
	}
	if ttlSecond < 0 {
		return 0, &APIError{
			Type:   RequestErr,
			Reason: "TTL should be >= 0",
		}
	}
	query := url.Values{}
	query.Add("limit", strconv.FormatInt(limit, 10))
	query.Add("ttl", strconv.FormatInt(ttlSecond, 10))
	req, err := c.getReq(http.MethodPut, path.Join(queue, "deadletter"), query, nil)
	if err != nil {
		return 0, &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
	resp, err := c.httpCli.Do(req)
	if err != nil {
		return 0, &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, &APIError{
			Type:      ResponseErr,
			Reason:    parseResponseError(resp),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	var respData struct {
		Count int `json:"count"`
	}
	err = json.Unmarshal(respBytes, &respData)
	if err != nil {
		return 0, &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	return respData.Count, nil
}

func (c *LmstfyClient) DeleteDeadLetter(queue string, limit int64) *APIError {
	if limit <= 0 {
		return &APIError{
			Type:   RequestErr,
			Reason: "limit should be > 0",
		}
	}
	query := url.Values{}
	query.Add("limit", strconv.FormatInt(limit, 10))
	req, err := c.getReq(http.MethodDelete, path.Join(queue, "deadletter"), query, nil)
	if err != nil {
		return &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
	resp, err := c.httpCli.Do(req)
	if err != nil {
		return &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return &APIError{
			Type:      ResponseErr,
			Reason:    parseResponseError(resp),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	return nil
}

func discardResponseBody(resp io.ReadCloser) {
	// discard response body, to make this connection reusable in the http connection pool
	ioutil.ReadAll(resp)
}

func parseResponseError(resp *http.Response) string {
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf("Invalid response: %s", err)
	}
	var errData struct {
		Error string `json:"error"`
	}
	err = json.Unmarshal(respBytes, &errData)
	if err != nil {
		return fmt.Sprintf("Invalid JSON: %s", err)
	}
	return fmt.Sprintf("[%d]%s", resp.StatusCode, errData.Error)
}
