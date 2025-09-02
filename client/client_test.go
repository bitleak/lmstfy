package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseSchemeFromURL(t *testing.T) {
	schemeCases := map[string]string{
		"http://abc.com":  "http",
		"https://abc.com": "https",
		"abc.com":         "http",
	}
	for rawURL, expectedScheme := range schemeCases {
		cli := NewLmstfyClient(rawURL, 0, "", "")
		if cli.scheme != expectedScheme {
			t.Errorf("scheme %s was expceted, but got %s", expectedScheme, cli.scheme)
		}
		if cli.Host != "abc.com" {
			t.Errorf("host %s was expceted, but got %s", "abc.com", cli.Host)
		}
	}
}

func TestLmstfyClient_Publish(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	cli.ConfigRetry(3, 5000)
	jobID, err := cli.Publish("test-publish", []byte("hello"), 0, 1, 0)
	if err != nil {
		t.Fatalf("Failed to send job: %s", err)
	}
	if jobID == "" {
		t.Fatal("Expected jobID")
	}
}

func TestLmstfyClient_RePublish(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	cli.ConfigRetry(3, 5000)
	jobID, err := cli.Publish("test-republish", []byte("hello"), 0, 1, 0)
	if err != nil {
		t.Fatalf("Failed to send job: %s", err)
	}
	if jobID == "" {
		t.Fatal("Expected jobID")
	}
	job, err := cli.Consume("test-republish", 5, 1)
	if err != nil {
		t.Fatalf("Failed to consume job: %s", err)
	}
	if jobID != job.ID || string(job.Data) != "hello" {
		t.Fatal("Mismatched data")
	}
	jobID, err = cli.RePublish(job, 10, 1, 0)
	if err != nil {
		t.Fatalf("Failed to re send job: %s", err)
	}
	if jobID == "" {
		t.Fatal("Expected a new jobID")
	}
	job, err = cli.Consume("test-republish", 5, 1)
	if err != nil {
		t.Fatalf("Failed to consume new job: %s", err)
	}
	if jobID != job.ID || string(job.Data) != "hello" || job.TTL != 10 {
		t.Fatal("Mismatched new data")
	}
}

func TestLmstfyClient_BatchPublish(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	cli.ConfigRetry(3, 5000)
	jobsData := []interface{}{
		"hello msg",
		123456,
		struct {
			Msg string `json:"msg"`
		}{Msg: "success"},
		[]string{"foo", "bar"},
		true,
	}

	jobIDs, err := cli.BatchPublish("test-batchpublish", jobsData, 10, 1, 0)
	if err != nil {
		t.Fatalf("Failed to send job: %s", err)
	}
	if len(jobIDs) != len(jobsData) {
		t.Fatal("Mismatch jobIDs count")
	}
	jobIDMap := map[string]int{}
	for idx, jobID := range jobIDs {
		jobIDMap[jobID] = idx
	}
	for i := 0; i < len(jobsData); i++ {
		job, err := cli.Consume("test-batchpublish", 5, 1)
		if err != nil {
			t.Fatalf("Failed to consume job: %s", err)
		}
		idx, ok := jobIDMap[job.ID]
		if !ok {
			t.Fatalf("Job not found")
		}
		jobData, _ := json.Marshal(jobsData[idx])
		if !bytes.Equal(job.Data, jobData) {
			t.Fatalf("Mismatched Job data")
		}
	}
}

func TestLmstfyClient_Consume(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-consume", []byte("hello"), 0, 1, 0)
	job, err := cli.Consume("test-consume", 10, 0)
	if err != nil {
		t.Fatalf("Failed to fetch job: %s", err)
	}
	if jobID != job.ID || string(job.Data) != "hello" {
		t.Fatal("Mismatched data")
	}
}

func TestLmstfyClient_PublishWithAttributes(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.PublishJob(&JobRequest{
		Queue: "test-publish-attributes",
		Data:  []byte("hello"),
		TTL:   10,
		Tries: 10,
		Delay: 0,
		Attributes: map[string]string{
			"hello": "world",
			"foo":   "bar",
		},
	})
	job, err := cli.Consume("test-publish-attributes", 10, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, jobID, job.ID)
	require.Equal(t, "hello", string(job.Data))
	require.Len(t, job.Attributes, 2)
	require.Equal(t, "world", job.Attributes["hello"])
	require.Equal(t, "bar", job.Attributes["foo"])
}

func TestLmstfyClient_BatchConsume(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobMap := map[string]bool{}
	for i := 0; i < 4; i++ {
		jobID, _ := cli.Publish("test-batchconsume", []byte("hello"), 0, 1, 0)
		jobMap[jobID] = true
	}

	queues := []string{"test-batchconsume"}
	jobs, err := cli.BatchConsume(queues, 3, 3, 3)
	if err != nil {
		t.Fatalf("Failed to fetch job: %s", err)
	}
	if len(jobs) != 3 {
		t.Fatal("Mismatched job count")
	}
	for _, job := range jobs {
		if !jobMap[job.ID] || string(job.Data) != "hello" {
			t.Fatal("Mismatched data")
		}
	}

	jobs, err = cli.BatchConsume(queues, 3, 3, 3)
	if err != nil {
		t.Fatalf("Failed to fetch job: %s", err)
	}
	if len(jobs) != 1 {
		t.Fatal("Mismatched job count")
	}
	for _, job := range jobs {
		if !jobMap[job.ID] || string(job.Data) != "hello" {
			t.Fatal("Mismatched data")
		}
	}

	now := time.Now()
	jobs, err = cli.BatchConsume(queues, 3, 3, 3)
	if err != nil {
		t.Fatalf("Failed to fetch job: %s", err)
	}
	if len(jobs) != 0 {
		t.Fatal("Mismatched job count")
	}
	if time.Now().Sub(now) < 3*time.Second {
		t.Fatal("Mismatched timeout second")
	}
}

func TestLmstfyClient_Ack(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	cli.Publish("test-ack", []byte("hello"), 0, 1, 0)
	job, _ := cli.Consume("test-ack", 10, 0)
	err := cli.Ack("test-finish", job.ID)
	if err != nil {
		t.Fatalf("Failed to finish a job: %s", err)
	}
}

func TestLmstfyClient_ConsumeFromQueues(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	cli.Publish("test-multi-consume1", []byte("hello1"), 0, 1, 0)
	jobID, _ := cli.Publish("test-multi-consume2", []byte("hello2"), 0, 1, 0)
	job, err := cli.ConsumeFromQueues(10, 1, "test-multi-consume2", "test-multi-consume1")
	if err != nil {
		t.Fatalf("Failed to fetch job: %s", err)
	}
	if job.Queue != "test-multi-consume2" || jobID != job.ID || string(job.Data) != "hello2" {
		t.Fatal("Mismatched data")
	}
}

func TestLmstfyClient_QueueSize(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	cli.Publish("test-queue-size", []byte("hello"), 0, 1, 0)
	cli.Publish("test-queue-size", []byte("hello"), 0, 1, 0)
	size, err := cli.QueueSize("test-queue-size")
	if err != nil {
		t.Fatalf("Failed to get queue size: %s", err)
	}
	if size != 2 {
		t.Fatal("Expected queue size == 2")
	}
}

func TestLmstfyClient_PeekQueue(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-peek-queue", []byte("hello1"), 0, 1, 0)
	cli.Publish("test-peek-queue", []byte("hello2"), 0, 1, 0)
	job, err := cli.PeekQueue("test-peek-queue")
	if err != nil {
		t.Fatalf("Failed to peek queue: %s", err)
	}
	if jobID != job.ID || string(job.Data) != "hello1" {
		t.Fatal("Mismatched data")
	}

	size, _ := cli.QueueSize("test-peek-queue")
	if size != 2 {
		t.Fatal("Expected queue size == 2")
	}
}

func TestLmstfyClient_PeekJob(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-peek-job", []byte("hello1"), 0, 1, 0)
	cli.Publish("test-peek-job", []byte("hello2"), 0, 1, 0)
	job, err := cli.PeekJob("test-peek-job", jobID)
	if err != nil {
		t.Fatalf("Failed to peek job: %s", err)
	}
	if string(job.Data) != "hello1" {
		t.Fatal("Mismatched data")
	}
}

func TestLmstfyClient_PeekDeadLetter(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-peek-deadletter", []byte("hello1"), 0, 1, 0)
	cli.Consume("test-peek-deadletter", 1, 0)
	time.Sleep(2 * time.Second) // wait til TTR expires
	size, head, err := cli.PeekDeadLetter("test-peek-deadletter")
	if err != nil {
		t.Fatalf("Failed to peek dead letter: %s", err)
	}
	if size != 1 && head != jobID {
		t.Fatal("Mismatched data")
	}
}

func TestLmstfyClient_RespawnDeadLetter(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-respawn-deadletter", []byte("hello1"), 0, 1, 0)
	cli.Consume("test-respawn-deadletter", 1, 0)
	time.Sleep(2 * time.Second) // wait til TTR expires
	count, err := cli.RespawnDeadLetter("test-respawn-deadletter", 2, 120)
	if err != nil {
		t.Fatalf("Failed to respawn deadletter: %s", err)
	}
	if count != 1 {
		t.Fatal("Mismatched deadletter size")
	}
	job, _ := cli.Consume("test-respawn-deadletter", 1, 0)
	if string(job.Data) != "hello1" || jobID != job.ID {
		t.Fatal("Mismatched data")
	}
}

func TestLmstfyClient_DeleteDeadLetter(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-delete-deadletter", []byte("hello1"), 0, 1, 0)
	cli.Consume("test-delete-deadletter", 1, 0)
	time.Sleep(2 * time.Second) // wait til TTR expires
	err := cli.DeleteDeadLetter("test-delete-deadletter", 2)
	if err != nil {
		t.Fatalf("Failed to delete deadletter: %s", err)
	}
	job, err := cli.PeekJob("test-delete-deadletter", jobID)
	if err != nil || (job != nil && job.Data != nil) {
		t.Fatal("delete deadletter failed")
	}
}

func TestLmstfyClient_ValidateConsumeTimeout(t *testing.T) {
	// Test with default HTTP client (600 seconds timeout)
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	
	// Test valid timeout (should return nil)
	err := cli.validateConsumeTimeout(300)
	if err != nil {
		t.Fatalf("Expected nil for valid timeout, got: %v", err)
	}
	
	// Test zero timeout (should return nil)
	err = cli.validateConsumeTimeout(0)
	if err != nil {
		t.Fatalf("Expected nil for zero timeout, got: %v", err)
	}
	
	// Test invalid timeout (should return error)
	err = cli.validateConsumeTimeout(600)
	if err == nil {
		t.Fatal("Expected error for timeout >= HTTP timeout")
	}
	expectedMsg := "consume timeout (600 seconds) must be less than HTTP client timeout (600 seconds)"
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Reason != expectedMsg {
		t.Fatalf("Expected error message '%s', got '%v'", expectedMsg, err)
	}
}

func TestLmstfyClient_ConsumeWithTimeoutValidation(t *testing.T) {
	// Test with custom HTTP client with short timeout
	shortHTTPClient := &http.Client{
		Timeout: 5 * time.Second,
	}
	cli := NewLmstfyWithClient(shortHTTPClient, Host, Port, Namespace, Token)
	
	// Test consume with valid timeout
	job, err := cli.Consume("test-timeout-validation", 10, 3)
	if err != nil {
		t.Fatalf("Consume should succeed with valid timeout: %v", err)
	}
	if job != nil {
		cli.Ack("test-timeout-validation", job.ID)
	}
	
	// Test consume with invalid timeout (should fail)
	_, err = cli.Consume("test-timeout-validation", 10, 6)
	if err == nil {
		t.Fatal("Consume should fail with timeout >= HTTP timeout")
	}
	expectedMsg := "consume timeout (6 seconds) must be less than HTTP client timeout (5 seconds)"
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Reason != expectedMsg {
		t.Fatalf("Expected error message '%s', got '%v'", expectedMsg, err)
	}
}

func TestLmstfyClient_BatchConsumeWithTimeoutValidation(t *testing.T) {
	// Test with custom HTTP client with short timeout
	shortHTTPClient := &http.Client{
		Timeout: 5 * time.Second,
	}
	cli := NewLmstfyWithClient(shortHTTPClient, Host, Port, Namespace, Token)
	
	// Test batch consume with valid timeout
	queues := []string{"test-batch-timeout-validation"}
	jobs, err := cli.BatchConsume(queues, 3, 10, 3)
	if err != nil {
		t.Fatalf("BatchConsume should succeed with valid timeout: %v", err)
	}
	
	// Ack any jobs that were returned
	for _, job := range jobs {
		cli.Ack("test-batch-timeout-validation", job.ID)
	}
	
	// Test batch consume with invalid timeout (should fail)
	_, err = cli.BatchConsume(queues, 3, 10, 6)
	if err == nil {
		t.Fatal("BatchConsume should fail with timeout >= HTTP timeout")
	}
	expectedMsg := "consume timeout (6 seconds) must be less than HTTP client timeout (5 seconds)"
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Reason != expectedMsg {
		t.Fatalf("Expected error message '%s', got '%v'", expectedMsg, err)
	}
}

func TestLmstfyClient_ConsumeFromQueuesWithTimeoutValidation(t *testing.T) {
	// Test with custom HTTP client with short timeout
	shortHTTPClient := &http.Client{
		Timeout: 5 * time.Second,
	}
	cli := NewLmstfyWithClient(shortHTTPClient, Host, Port, Namespace, Token)
	
	// Test consume from queues with valid timeout
	job, err := cli.ConsumeFromQueues(10, 3, "test-multi-queue-timeout-validation")
	if err != nil {
		t.Fatalf("ConsumeFromQueues should succeed with valid timeout: %v", err)
	}
	if job != nil {
		cli.Ack("test-multi-queue-timeout-validation", job.ID)
	}
	
	// Test consume from queues with invalid timeout (should fail)
	_, err = cli.ConsumeFromQueues(10, 6, "test-multi-queue-timeout-validation")
	if err == nil {
		t.Fatal("ConsumeFromQueues should fail with timeout >= HTTP timeout")
	}
	expectedMsg := "consume timeout (6 seconds) must be less than HTTP client timeout (5 seconds)"
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Reason != expectedMsg {
		t.Fatalf("Expected error message '%s', got '%v'", expectedMsg, err)
	}
}

func TestLmstfyClient_GetHTTPTimeout(t *testing.T) {
	// Test with default HTTP client
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	timeout := cli.getHTTPTimeout()
	if timeout != 600*time.Second {
		t.Fatalf("Expected 600 seconds timeout for default client, got %v", timeout)
	}
	
	// Test with custom HTTP client
	customTimeout := 30 * time.Second
	customClient := &http.Client{
		Timeout: customTimeout,
	}
	cli2 := NewLmstfyWithClient(customClient, Host, Port, Namespace, Token)
	timeout = cli2.getHTTPTimeout()
	if timeout != customTimeout {
		t.Fatalf("Expected %v timeout for custom client, got %v", customTimeout, timeout)
	}
}
