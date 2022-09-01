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

	"github.com/magiconair/properties/assert"

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
	body, _ := publishTestJob("ns", "q2", 0, 0)

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
		Tries     int `json:"remain_tries"`
		TTL       int `json:"ttl"`
	}
	err = json.Unmarshal(resp.Body.Bytes(), &data)
	if err != nil {
		t.Fatalf("Failed to decode response: %s", err)
	}
	assert.Equal(t, data.TTL, 0)
	assert.Equal(t, 0, data.Tries)
	if !bytes.Equal(data.Data, body) {
		t.Fatalf("Mismatched job data")
	}
}

func TestNoBlockingConsumeMulti(t *testing.T) {
	body, jobID := publishTestJob("ns", "q4", 0, 60)
	query := url.Values{}
	query.Add("ttr", "10")
	query.Add("timeout", "0")
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

func TestConsumeMulti(t *testing.T) {
	body, jobID := publishTestJob("ns", "q4", 2, 60)
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
	_, jobID := publishTestJob("ns", "q6", 1, 60)

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
	_, jobID := publishTestJob("ns", "q7", 0, 60)

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
	assert.Equal(t, jobID, data.JobID)
}

func TestSize(t *testing.T) {
	publishTestJob("ns", "q8", 0, 60)

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
	_, jobID := publishTestJob("ns", "q9", 0, 60)

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
	_, jobID := publishTestJob("ns", "q10", 0, 60)
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
	_, jobID := publishTestJob("ns", "q11", 0, 60)
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
	_, jobID := publishTestJob("ns", "q12", 0, 60)
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
	publishTestJob("ns", "q13", 0, 60)

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
		body, jobID := publishTestJob("ns", "q14", 0, 60)
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
		Tries     int `json:"remain_tries"`
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
		assert.Equal(t, 0, job.Tries)
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
	_, jobID := publishTestJob("ns", "q15", 0, 60)
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

func TestRePublish(t *testing.T) {
	data, jobID := publishTestJob("ns", "q16", 0, 60)
	query := url.Values{}
	query.Add("delay", "0")
	query.Add("ttl", "10")
	query.Add("tries", "1")

	targetURL := fmt.Sprintf("http://localhost/api/ns/q16/job/%s?%s", jobID, query.Encode())
	body := bytes.NewReader(data)
	req, err := http.NewRequest("PUT", targetURL, body)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.PUT("/api/:namespace/:queue/job/:job_id", handlers.Publish)
	e.HandleContext(c)
	if resp.Code != http.StatusCreated {
		t.Fatal("Failed to publish")
	}
	job, err := engine.GetEngine("").Consume("ns", []string{"q16"}, 5, 2)
	if err != nil || string(job.Body()) != string(data) || job.TTL() != 10 || job.ID() == jobID {
		t.Fatal("Failed to republish", job, err)
	}

}

func TestPublishBulk(t *testing.T) {
	query := url.Values{}
	query.Add("delay", "0")
	query.Add("ttl", "10")
	query.Add("tries", "1")
	targetUrl := fmt.Sprintf("http://localhost/api/ns/q17/bulk?%s", query.Encode())
	jobsData := []interface{}{
		"hello msg",
		123456,
		struct {
			Msg string `json:"msg"`
		}{Msg: "success"},
		[]string{"foo", "bar"},
		true,
	}
	bodyData, _ := json.Marshal(jobsData)
	body := bytes.NewReader(bodyData)
	req, err := http.NewRequest("PUT", targetUrl, body)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	e.PUT("/api/:namespace/:queue/bulk", handlers.PublishBulk)
	e.HandleContext(c)
	if resp.Code != http.StatusCreated {
		t.Fatal("Failed to publish")
	}
	var data struct {
		Msg    string
		JobIDs []string `json:"job_ids"`
	}
	err = json.Unmarshal(resp.Body.Bytes(), &data)
	if err != nil {
		t.Fatalf("Failed to decode response: %s", err)
	}
	if len(data.JobIDs) != len(jobsData) {
		t.Fatalf("Mismatched job count")
	}
	jobIDMap := map[string]int{}
	for idx, jobID := range data.JobIDs {
		jobIDMap[jobID] = idx
	}
	for i := 0; i < len(jobsData); i++ {
		body, jobID := consumeTestJob("ns", "q17", 0, 1)
		idx, ok := jobIDMap[jobID]
		if !ok {
			t.Fatalf("Job not found")
		}
		jobData, _ := json.Marshal(jobsData[idx])
		if !bytes.Equal(body, jobData) {
			t.Fatalf("Mismatched Job data")
		}
	}
}

func publishTestJob(ns, q string, delay, ttl uint32) (body []byte, jobID string) {
	e := engine.GetEngine("")
	body = make([]byte, 10)
	if _, err := rand.Read(body); err != nil {
		panic(err)
	}
	j := engine.NewJob(ns, q, body, ttl, delay, 1, "")
	jobID, _ = e.Publish(j)
	return body, jobID
}

func consumeTestJob(ns, q string, ttr, timeout uint32) (body []byte, jobID string) {
	e := engine.GetEngine("")
	job, _ := e.Consume(ns, []string{q}, ttr, timeout)
	if job == nil {
		return nil, ""
	}
	return job.Body(), job.ID()
}
