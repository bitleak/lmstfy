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
	Namespace string `json:"namespace"`
	Queue     string `json:"queue"`
	Data      []byte `json:"data"`
	ID        string `json:"job_id"`
	TTL       int64  `json:"ttl"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

type LmstfyClient struct {
	Namespace string
	Token     string
	Host      string
	Port      int

	retry   int // retry when Publish failed (only some situations are worth of retrying)
	backOff int // millisecond
	httpCli *http.Client
}

const (
	maxReadTimeout      = 600 // second
	maxBatchConsumeSize = 100
)

func NewLmstfyClient(host string, port int, namespace, token string) *LmstfyClient {
	cli := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).DialContext,
		},
		Timeout: maxReadTimeout * time.Second,
	}
	return &LmstfyClient{
		Namespace: namespace,
		Token:     token,
		Host:      host,
		Port:      port,

		httpCli: cli,
	}
}

func (c *LmstfyClient) ConfigRetry(retryCount int, backOffMillisecond int) {
	c.retry = retryCount
	c.backOff = backOffMillisecond
}

func (c *LmstfyClient) getReq(method, relativePath string, query url.Values, body []byte) (req *http.Request, err error) {
	targetUrl := url.URL{
		Scheme:   "http",
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:     path.Join("/api", c.Namespace, relativePath),
		RawQuery: query.Encode(),
	}
	if body == nil {
		req, err = http.NewRequest(method, targetUrl.String(), nil)
		if err != nil {
			return
		}
		req.Header.Add("X-Token", c.Token)
		return
	}
	req, err = http.NewRequest(method, targetUrl.String(), bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Add("X-Token", c.Token)
	return
}

// Publish a new job to the queue.
//   - ttlSecond is the time-to-live of the job. If it's zero, job won't expire; if it's positive, the value is the TTL.
//   - tries is the maximum times the job can be fetched.
//   - delaySecond is the duration before the job is released for consuming. When it's zero, no delay is applied.
func (c *LmstfyClient) Publish(queue string, data []byte, ttlSecond uint32, tries uint16, delaySecond uint32) (jobID string, e error) {
	return c.publish(queue, "", data, ttlSecond, tries, delaySecond)
}

// RePublish delete(ack) the job of the queue and publish the job again.
//   - ttlSecond is the time-to-live of the job. If it's zero, job won't expire; if it's positive, the value is the TTL.
//   - tries is the maximum times the job can be fetched.
//   - delaySecond is the duration before the job is released for consuming. When it's zero, no delay is applied.
func (c *LmstfyClient) RePublish(job *Job, ttlSecond uint32, tries uint16, delaySecond uint32) (jobID string, e error) {
	return c.publish(job.Queue, job.ID, job.Data, ttlSecond, tries, delaySecond)
}

func (c *LmstfyClient) publish(queue, ackJobID string, data []byte, ttlSecond uint32, tries uint16, delaySecond uint32) (jobID string, e error) {
	query := url.Values{}
	query.Add("ttl", strconv.FormatUint(uint64(ttlSecond), 10))
	query.Add("tries", strconv.FormatUint(uint64(tries), 10))
	query.Add("delay", strconv.FormatUint(uint64(delaySecond), 10))
	retryCount := 0
	relativePath := queue
	if ackJobID != "" {
		relativePath = path.Join(relativePath, "job", ackJobID)
	}
RETRY:
	req, err := c.getReq(http.MethodPut, relativePath, query, data)
	if err != nil {
		return "", &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}

	resp, err := c.httpCli.Do(req)
	if err != nil {
		if retryCount < c.retry {
			time.Sleep(time.Duration(c.backOff) * time.Millisecond)
			retryCount++
			goto RETRY
		}
		return "", &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
	if resp.StatusCode != http.StatusCreated {
		resp.Body.Close()
		if resp.StatusCode >= 500 && retryCount < c.retry {
			time.Sleep(time.Duration(c.backOff) * time.Millisecond)
			retryCount++
			goto RETRY
		}
		return "", &APIError{
			Type:      ResponseErr,
			Reason:    parseResponseError(resp),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		if retryCount < c.retry {
			time.Sleep(time.Duration(c.backOff) * time.Millisecond)
			retryCount++
			goto RETRY
		}
		return "", &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	var respData struct {
		JobID string `json:"job_id"`
	}
	err = json.Unmarshal(respBytes, &respData)
	if err != nil {
		return "", &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	return respData.JobID, nil
}

// Consume a job. Consuming will decrease the job's tries by 1 first.
//   - ttrSecond is the time-to-run of the job. If the job is not finished before the TTR expires,
//     the job will be released for consuming again if the `(tries - 1) > 0`.
//   - timeoutSecond is the long-polling wait time. If it's zero, this method will return immediately
//     with or without a job; if it's positive, this method would polling for new job until timeout.
func (c *LmstfyClient) Consume(queue string, ttrSecond, timeoutSecond uint32) (job *Job, e error) {
	if ttrSecond <= 0 {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: "TTR should be > 0",
		}
	}
	if timeoutSecond < 0 || timeoutSecond >= maxReadTimeout {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: fmt.Sprintf("timeout should be >= 0 && < %d", maxReadTimeout),
		}
	}
	query := url.Values{}
	query.Add("ttr", strconv.FormatUint(uint64(ttrSecond), 10))
	query.Add("timeout", strconv.FormatUint(uint64(timeoutSecond), 10))
	req, err := c.getReq(http.MethodGet, queue, query, nil)
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

// BatchConsume consume some jobs. Consuming will decrease these jobs tries by 1 first.
//   - ttrSecond is the time-to-run of these jobs. If these jobs are not finished before the TTR expires,
//     these job will be released for consuming again if the `(tries - 1) > 0`.
//   - count is the job count of this consume. If it's zero or over 100, this method will return an error.
//     If it's positive, this method would return some jobs, and it's count is between 0 and count.
func (c *LmstfyClient) BatchConsume(queue string, count, ttrSecond, timeoutSecond uint32) (jobs []*Job, e error) {
	if ttrSecond <= 0 {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: "TTR should be > 0",
		}
	}
	if count <= 0 || count > maxBatchConsumeSize {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: "COUNT should be > 0",
		}
	}
	if timeoutSecond < 0 || timeoutSecond >= maxReadTimeout {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: fmt.Sprintf("timeout should be >= 0 && < %d", maxReadTimeout),
		}
	}

	query := url.Values{}
	query.Add("ttr", strconv.FormatUint(uint64(ttrSecond), 10))
	query.Add("count", strconv.FormatUint(uint64(count), 10))
	query.Add("timeout", strconv.FormatUint(uint64(timeoutSecond), 10))
	req, err := c.getReq(http.MethodGet, queue, query, nil)
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
	if count == 1 {
		job := &Job{}
		err = json.Unmarshal(respBytes, job)
		if err != nil {
			return nil, &APIError{
				Type:      ResponseErr,
				Reason:    err.Error(),
				RequestID: resp.Header.Get("X-Request-ID"),
			}
		}
		return []*Job{job}, nil
	}
	jobs = []*Job{}
	err = json.Unmarshal(respBytes, &jobs)
	if err != nil {
		return nil, &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	return jobs, nil
}

// Consume from multiple queues with priority.
// The order of the queues in the params implies the priority. eg.
//   ConsumeFromQueues(120, 5, "queue-a", "queue-b", "queue-c")
// if all the queues have jobs to be fetched, the job in `queue-a` will be return.
func (c *LmstfyClient) ConsumeFromQueues(ttrSecond, timeoutSecond uint32, queues ...string) (job *Job, e error) {
	if ttrSecond <= 0 {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: "TTR should be > 0",
		}
	}
	if timeoutSecond <= 0 || timeoutSecond >= maxReadTimeout {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: fmt.Sprintf("timeout must be > 0 && < %d when fetch from multiple queues", maxReadTimeout),
		}
	}
	query := url.Values{}
	query.Add("ttr", strconv.FormatUint(uint64(ttrSecond), 10))
	query.Add("timeout", strconv.FormatUint(uint64(timeoutSecond), 10))
	req, err := c.getReq(http.MethodGet, strings.Join(queues, ","), query, nil)
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

// Mark a job as finished, so it won't be retried by others.
func (c *LmstfyClient) Ack(queue, jobID string) *APIError {
	req, err := c.getReq(http.MethodDelete, path.Join(queue, "job", jobID), nil, nil)
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

// Get queue size. how many jobs are ready for consuming
func (c *LmstfyClient) QueueSize(queue string) (int, *APIError) {
	req, err := c.getReq(http.MethodGet, path.Join(queue, "size"), nil, nil)
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
		Namespace string `json:"namespace"`
		Queue     string `json:"queue"`
		Size      int    `json:"size"`
	}
	err = json.Unmarshal(respBytes, &respData)
	if err != nil {
		return 0, &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	return respData.Size, nil
}

// Peek the job in the head of the queue
func (c *LmstfyClient) PeekQueue(queue string) (job *Job, e *APIError) {
	req, err := c.getReq(http.MethodGet, path.Join(queue, "peek"), nil, nil)
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
