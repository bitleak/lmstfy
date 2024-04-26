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
