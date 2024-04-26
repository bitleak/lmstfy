package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	maxReadTimeout      = 600 // second
	maxBatchConsumeSize = 100
)

// getReq generates an HTTP request
// - ctx is the context of the request, nil if no context
// - method is the HTTP method
// - relativePath is the relative path of the request
// - query is the query parameters
// - body is the request body, nil if no body to send
func (c *LmstfyClient) getReq(ctx context.Context, method, relativePath string, query url.Values, body []byte) (
	req *http.Request, err error) {
	targetUrl := (&url.URL{
		Scheme:   c.scheme,
		Host:     c.endpoint,
		Path:     path.Join("/api", c.Namespace, relativePath),
		RawQuery: query.Encode(),
	}).String()

	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	if ctx != nil {
		req, err = http.NewRequestWithContext(ctx, method, targetUrl, bodyReader)
	} else {
		req, err = http.NewRequest(method, targetUrl, bodyReader)
	}
	if err != nil {
		return nil, err
	}

	req.Header.Add("X-Token", c.Token)
	return req, nil
}

// publish publishes a job to the given queue
func (c *LmstfyClient) publish(ctx context.Context, queue, ackJobID string, data []byte, ttlSecond uint32, tries uint16,
	delaySecond uint32) (jobID string, e error) {
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
	req, err := c.getReq(ctx, http.MethodPut, relativePath, query, data)
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
		if resp.StatusCode >= 500 && retryCount < c.retry {
			time.Sleep(time.Duration(c.backOff) * time.Millisecond)
			retryCount++
			resp.Body.Close()
			goto RETRY
		}
		defer resp.Body.Close()
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

// batchPublish publish lots of jobs at one time
//   - ttlSecond is the time-to-live of the job. If it's zero, the job won't be expired; if it's positive, the value is the TTL.
//   - tries is the maximum retries that the job can be reached
//   - delaySecond is the duration before the job is released for consuming. When it's zero, no delay is applied.
func (c *LmstfyClient) batchPublish(ctx context.Context, queue string, jobs []interface{}, ttlSecond uint32, tries uint16,
	delaySecond uint32) (jobIDs []string, e error) {
	query := url.Values{}
	query.Add("ttl", strconv.FormatUint(uint64(ttlSecond), 10))
	query.Add("tries", strconv.FormatUint(uint64(tries), 10))
	query.Add("delay", strconv.FormatUint(uint64(delaySecond), 10))
	retryCount := 0
	relativePath := path.Join(queue, "bulk")
	data, err := json.Marshal(jobs)
	if err != nil {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
RETRY:
	req, err := c.getReq(ctx, http.MethodPut, relativePath, query, data)
	if err != nil {
		return nil, &APIError{
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
		return nil, &APIError{
			Type:   RequestErr,
			Reason: err.Error(),
		}
	}
	if resp.StatusCode != http.StatusCreated {
		if resp.StatusCode >= 500 && retryCount < c.retry {
			time.Sleep(time.Duration(c.backOff) * time.Millisecond)
			retryCount++
			resp.Body.Close()
			goto RETRY
		}
		defer resp.Body.Close()
		return nil, &APIError{
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
		return nil, &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	var respData struct {
		JobIDs []string `json:"job_ids"`
	}
	err = json.Unmarshal(respBytes, &respData)
	if err != nil {
		return nil, &APIError{
			Type:      ResponseErr,
			Reason:    err.Error(),
			RequestID: resp.Header.Get("X-Request-ID"),
		}
	}
	return respData.JobIDs, nil
}

// consume a job. Consuming will decrease the job's tries by 1 first.
//   - ttrSecond is the time-to-run of the job. If the job is not finished before the TTR expires,
//     the job will be released for consuming again if the `(tries - 1) > 0`.
//   - timeoutSecond is the long-polling wait time. If it's zero, this method will return immediately
//     with or without a job; if it's positive, this method would poll for a new job until timeout.
//   - free_tries was used to determine whether to decrease the tries or not when consuming, if the tries
//     decrease to 0, the job would move into a dead letter. Default was false.
func (c *LmstfyClient) consume(ctx context.Context, queue string, ttrSecond, timeoutSecond uint32, freezeTries bool) (
	job *Job, e error) {
	if strings.TrimSpace(queue) == "" {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: "Queue name shouldn't be empty",
		}
	}
	if ttrSecond <= 0 {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: "TTR should be > 0",
		}
	}
	if timeoutSecond >= maxReadTimeout {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: fmt.Sprintf("timeout should be < %d", maxReadTimeout),
		}
	}
	query := url.Values{}
	query.Add("ttr", strconv.FormatUint(uint64(ttrSecond), 10))
	query.Add("timeout", strconv.FormatUint(uint64(timeoutSecond), 10))
	query.Add("freeze_tries", strconv.FormatBool(freezeTries))
	req, err := c.getReq(ctx, http.MethodGet, queue, query, nil)
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
		if c.errorOnNilJob {
			return nil, &APIError{
				Type:      ResponseErr,
				Reason:    "no job or queue was not found",
				RequestID: resp.Header.Get("X-Request-ID"),
			}
		}
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

// batchConsume consumes some jobs. Consuming will decrease these jobs tries by 1 first.
//   - ttrSecond is the time-to-run of these jobs. If these jobs are not finished before the TTR expires,
//     these jobs will be released to consume again if the `(tries - 1) > 0`.
//   - count is the job count of this consume. If it's zero or over 100, this method will return an error.
//     If it's positive, this method would return some jobs, and its count is between 0 and count.
//   - freezeTries was used to determine whether to decrease the tries or not when consuming, if the tries
//     decrease to 0, the job would move into a dead letter. Default was false.
func (c *LmstfyClient) batchConsume(ctx context.Context, queues []string, count, ttrSecond, timeoutSecond uint32,
	freezeTries bool) (jobs []*Job, e error) {
	if len(queues) == 0 {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: "At least one queue was required",
		}
	}
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
	if timeoutSecond >= maxReadTimeout {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: fmt.Sprintf("timeout should be < %d", maxReadTimeout),
		}
	}

	query := url.Values{}
	query.Add("ttr", strconv.FormatUint(uint64(ttrSecond), 10))
	query.Add("count", strconv.FormatUint(uint64(count), 10))
	query.Add("timeout", strconv.FormatUint(uint64(timeoutSecond), 10))
	query.Add("freeze_tries", strconv.FormatBool(freezeTries))
	req, err := c.getReq(ctx, http.MethodGet, strings.Join(queues, ","), query, nil)
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

func (c *LmstfyClient) consumeFromQueues(ctx context.Context, ttrSecond, timeoutSecond uint32, freezeTries bool,
	queues ...string) (job *Job, e error) {
	if len(queues) == 0 {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: "At least one queue was required",
		}
	}
	if ttrSecond <= 0 {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: "TTR should be > 0",
		}
	}
	if timeoutSecond >= maxReadTimeout {
		return nil, &APIError{
			Type:   RequestErr,
			Reason: fmt.Sprintf("timeout must be < %d when fetch from multiple queues", maxReadTimeout),
		}
	}
	query := url.Values{}
	query.Add("ttr", strconv.FormatUint(uint64(ttrSecond), 10))
	query.Add("timeout", strconv.FormatUint(uint64(timeoutSecond), 10))
	query.Add("freeze_tries", strconv.FormatBool(freezeTries))
	req, err := c.getReq(ctx, http.MethodGet, strings.Join(queues, ","), query, nil)
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

// ack Marks a job as finished, so it won't be retried by others.
func (c *LmstfyClient) ack(ctx context.Context, queue, jobID string) *APIError {
	req, err := c.getReq(ctx, http.MethodDelete, path.Join(queue, "job", jobID), nil, nil)
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

// queueSize Gets the queue size. it means how many jobs are ready to consume
func (c *LmstfyClient) queueSize(ctx context.Context, queue string) (int, *APIError) {
	req, err := c.getReq(ctx, http.MethodGet, path.Join(queue, "size"), nil, nil)
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

// peekQueue Peek the job in the head of the queue
func (c *LmstfyClient) peekQueue(ctx context.Context, queue string) (job *Job, e *APIError) {
	req, err := c.getReq(ctx, http.MethodGet, path.Join(queue, "peek"), nil, nil)
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

// peekJob Peek a specific job data
func (c *LmstfyClient) peekJob(ctx context.Context, queue, jobID string) (job *Job, e *APIError) {
	req, err := c.getReq(ctx, http.MethodGet, path.Join(queue, "job", jobID), nil, nil)
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

// peekDeadLetter Peek the dead letter of the queue
func (c *LmstfyClient) peekDeadLetter(ctx context.Context, queue string) (
	deadLetterSize int, deadLetterHead string, e *APIError) {
	req, err := c.getReq(ctx, http.MethodGet, path.Join(queue, "deadletter"), nil, nil)
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

func (c *LmstfyClient) respawnDeadLetter(ctx context.Context, queue string, limit, ttlSecond int64) (
	count int, e *APIError) {
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
	req, err := c.getReq(ctx, http.MethodPut, path.Join(queue, "deadletter"), query, nil)
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
	_, _ = ioutil.ReadAll(resp)
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
