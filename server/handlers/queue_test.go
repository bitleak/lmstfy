package handlers_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/server/handlers"
)

func TestPublish(t *testing.T) {
	query := url.Values{}
	query.Add("delay", "5")
	query.Add("ttl", "10")
	query.Add("tries", "1")
	targetUrl := fmt.Sprintf("http://localhost/api/ns/q1?%s", query.Encode())
	body := strings.NewReader("hello msg")
	req, err := http.NewRequest("PUT", targetUrl, body)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.PUT("/api/:namespace/:queue", handlers.Publish)
	e.HandleContext(c)
	if resp.Code != http.StatusCreated {
		t.Fatal("Failed to publish")
	}
}

func TestConsume(t *testing.T) {
	body, _ := publishTestJob("ns", "q2", 0)

	query := url.Values{}
	query.Add("ttr", "10")
	query.Add("timeout", "2")
	targetUrl := fmt.Sprintf("http://localhost/api/ns/q2?%s", query.Encode())
	req, err := http.NewRequest("GET", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.GET("/api/:namespace/:queue", handlers.Consume)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Fatal("Failed to consume")
	}
	var data struct {
		Msg       string
		Namespace string
		Queue     string
		JobID     string `json:"job_id"`
		Data      []byte
	}
	err = json.Unmarshal(resp.Body.Bytes(), &data)
	if err != nil {
		t.Fatalf("Failed to decode response: %s", err)
	}
	if !bytes.Equal(data.Data, body) {
		t.Fatalf("Mismatched job data")
	}
}

func TestConsumeMulti(t *testing.T) {
	body, jobID := publishTestJob("ns", "q4", 2)

	query := url.Values{}
	query.Add("ttr", "10")
	query.Add("timeout", "3")
	targetUrl := fmt.Sprintf("http://localhost/api/ns/q3,q4,q5?%s", query.Encode())
	req, err := http.NewRequest("GET", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateMultiConsume, handlers.SetupQueueEngine)
	e.GET("/api/:namespace/:queue", handlers.Consume)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Fatal("Failed to consume")
	}
	var data struct {
		Msg       string
		Namespace string
		Queue     string
		JobID     string `json:"job_id"`
		Data      []byte
	}
	err = json.Unmarshal(resp.Body.Bytes(), &data)
	if err != nil {
		t.Fatalf("Failed to decode response: %s", err)
	}
	if data.JobID != jobID || !bytes.Equal(data.Data, body) || data.Queue != "q4" {
		t.Fatalf("Mismatched job data")
	}
}

func TestDelete(t *testing.T) {
	_, jobID := publishTestJob("ns", "q6", 1)

	targetUrl := fmt.Sprintf("http://localhost/api/ns/q6/job/%s", jobID)
	req, err := http.NewRequest("DELETE", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.DELETE("/api/:namespace/:queue/job/:job_id", handlers.Delete)
	e.HandleContext(c)
	if resp.Code != http.StatusNoContent {
		t.Fatal("Failed to delete")
	}

	_, jobID = consumeTestJob("ns", "q6", 10, 2)
	if jobID != "" {
		t.Fatal("Expected no job")
	}
}

func TestPeekQueue(t *testing.T) {
	_, jobID := publishTestJob("ns", "q7", 0)

	targetUrl := "http://localhost/api/ns/q7/peek"
	req, err := http.NewRequest("GET", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.GET("/api/:namespace/:queue/peek", handlers.PeekQueue)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Log(resp.Body.String())
		t.Fatal("Failed to peek queue")
	}

	var data struct {
		Namespace string
		Queue     string
		JobID     string `json:"job_id"`
		Data      []byte
	}
	err = json.Unmarshal(resp.Body.Bytes(), &data)
	if err != nil {
		t.Fatal("Failed to decode response")
	}
	if data.JobID != jobID {
		t.Log(resp.Body.String())
		t.Fatal("Mismatched job")
	}
}

func TestSize(t *testing.T) {
	publishTestJob("ns", "q8", 0)

	targetUrl := "http://localhost/api/ns/q8/size"
	req, err := http.NewRequest("GET", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.GET("/api/:namespace/:queue/size", handlers.Size)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Log(resp.Body.String())
		t.Fatal("Failed to get size")
	}

	var data struct {
		Namespace string
		Queue     string
		Size      int64
	}
	err = json.Unmarshal(resp.Body.Bytes(), &data)
	if err != nil {
		t.Fatal("Failed to decode response")
	}
	if data.Size != 1 {
		t.Log(resp.Body.String())
		t.Fatal("Mismatched queue size")
	}
}

func TestPeekJob(t *testing.T) {
	_, jobID := publishTestJob("ns", "q9", 0)

	targetUrl := fmt.Sprintf("http://localhost/api/ns/q9/job/%s", jobID)
	req, err := http.NewRequest("GET", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.GET("/api/:namespace/:queue/job/:job_id", handlers.PeekJob)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Log(resp.Body.String())
		t.Fatal("Failed to peek job")
	}

	var data struct {
		Namespace string
		Queue     string
		JobID     string `json:"job_id"`
		Data      []byte
	}
	err = json.Unmarshal(resp.Body.Bytes(), &data)
	if err != nil {
		t.Fatal("Failed to decode response")
	}
	if data.JobID != jobID {
		t.Log(resp.Body.String())
		t.Fatal("Mismatched job")
	}
}

func TestPeekDeadLetter(t *testing.T) {
	// Publish and consume a job without ACK(delete), so it will be move to deadletter ASAP
	_, jobID := publishTestJob("ns", "q10", 0)
	_, jobID2 := consumeTestJob("ns", "q10", 0, 1)
	if jobID != jobID2 {
		t.Fatal("Failed to setup dead job")
	}
	time.Sleep(time.Second)

	targetUrl := "http://localhost/api/ns/q10/deadletter"
	req, err := http.NewRequest("GET", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.GET("/api/:namespace/:queue/deadletter", handlers.PeekDeadLetter)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Log(resp.Body.String())
		t.Fatal("Failed to peek deadletter")
	}
}

func TestRespawnDeadLetter(t *testing.T) {
	// Publish and consume a job without ACK(delete), so it will be move to deadletter ASAP
	_, jobID := publishTestJob("ns", "q11", 0)
	_, jobID2 := consumeTestJob("ns", "q11", 0, 1)
	if jobID != jobID2 {
		t.Fatal("Failed to setup dead job")
	}
	time.Sleep(time.Second)

	targetUrl := "http://localhost/api/ns/q11/deadletter"
	req, err := http.NewRequest("PUT", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.PUT("/api/:namespace/:queue/deadletter", handlers.RespawnDeadLetter)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Log(resp.Body.String())
		t.Fatal("Failed to respawn job from deadletter")
	}

	_, jobID3 := consumeTestJob("ns", "q11", 0, 1)
	if jobID3 != jobID {
		t.Fatal("Expected the job to be respawned")
	}
}

func TestDeleteDeadLetter(t *testing.T) {
	// Publish and consume a job without ACK(delete), so it will be move to deadletter ASAP
	_, jobID := publishTestJob("ns", "q12", 0)
	_, jobID2 := consumeTestJob("ns", "q12", 0, 1)
	if jobID != jobID2 {
		t.Fatal("Failed to setup dead job")
	}
	time.Sleep(time.Second)

	targetUrl := "http://localhost/api/ns/q12/deadletter"
	req, err := http.NewRequest("DELETE", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.DELETE("/api/:namespace/:queue/deadletter", handlers.DeleteDeadLetter)
	e.HandleContext(c)
	if resp.Code != http.StatusNoContent {
		t.Log(resp.Body.String())
		t.Fatal("Failed to delete job from deadletter")
	}
}

func TestDestroyQueue(t *testing.T) {
	publishTestJob("ns", "q13", 0)

	targetUrl := "http://localhost/api/ns/q13"
	req, err := http.NewRequest("DELETE", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.DELETE("/api/:namespace/:queue", handlers.DestroyQueue)
	e.HandleContext(c)
	if resp.Code != http.StatusNoContent {
		t.Log(resp.Body.String())
		t.Fatal("Failed to delete job from deadletter")
	}
}

func TestBatchConsume(t *testing.T) {
	jobMap := map[string][]byte{}
	for i := 0; i < 4; i++ {
		body, jobID := publishTestJob("ns", "q14", 0)
		jobMap[jobID] = body
	}

	query := url.Values{}
	query.Add("ttr", "10")
	query.Add("count", "3")
	query.Add("timeout", "3")
	targetURL := fmt.Sprintf("http://localhost/api/ns/q14?%s", query.Encode())
	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.GET("/api/:namespace/:queue", handlers.Consume)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Fatal("Failed to consume")
	}
	var data []struct {
		Msg       string
		Namespace string
		Queue     string
		JobID     string `json:"job_id"`
		Data      []byte
	}
	err = json.Unmarshal(resp.Body.Bytes(), &data)
	if err != nil {
		t.Fatalf("Failed to decode response: %s", err)
	}
	if len(data) != 3 {
		t.Fatalf("Mismatched job count")
	}
	for _, job := range data {
		if body, ok := jobMap[job.JobID]; !ok || !bytes.Equal(job.Data, body) {
			t.Fatalf("Mismatched job data")
		}
	}

	c, e, resp = ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.GET("/api/:namespace/:queue", handlers.Consume)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Fatal("Failed to consume")
	}
	err = json.Unmarshal(resp.Body.Bytes(), &data)
	if err != nil {
		t.Fatalf("Failed to decode response: %s", err)
	}
	if len(data) != 1 {
		t.Fatalf("Mismatched job count")
	}
	for _, job := range data {
		if body, ok := jobMap[job.JobID]; !ok || !bytes.Equal(job.Data, body) {
			t.Fatalf("Mismatched job data")
		}
	}

	c, e, resp = ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.GET("/api/:namespace/:queue", handlers.Consume)
	e.HandleContext(c)
	if resp.Code != http.StatusNotFound {
		t.Fatal("Failed to consume")
	}
}

func TestSizeOfDeadLetter(t *testing.T) {
	_, jobID := publishTestJob("ns", "q15", 0)
	_, jobID2 := consumeTestJob("ns", "q15", 0, 1)
	if jobID != jobID2 {
		t.Fatal("Failed to setup dead job")
	}
	time.Sleep(time.Second)
	targetURL := "http://localhost/api/ns/q15/deadletter/size"
	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.GET("/api/:namespace/:queue/deadletter/size", handlers.GetDeadLetterSize)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Fatal("Failed to get size")
	}
	var data struct {
		Namespace string
		Queue     string
		Size      int
	}
	err = json.Unmarshal(resp.Body.Bytes(), &data)
	if err != nil {
		t.Fatalf("Failed to decode response: %s", err)
	}
	if data.Size != 1 {
		t.Fatalf("Mismatched deadletter queue size")
	}
}

func publishTestJob(ns, q string, delay uint32) (body []byte, jobID string) {
	e := engine.GetEngineByKind("redis", "")
	body = make([]byte, 10)
	if _, err := rand.Read(body); err != nil {
		panic(err)
	}
	jobID, _ = e.Publish(ns, q, body, 60, delay, 1)
	return body, jobID
}

func consumeTestJob(ns, q string, ttr, timeout uint32) (body []byte, jobID string) {
	e := engine.GetEngineByKind("redis", "")
	job, _ := e.Consume(ns, q, ttr, timeout)
	if job == nil {
		return nil, ""
	}
	return job.Body(), job.ID()
}
