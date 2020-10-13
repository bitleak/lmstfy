package handlers_test

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/server/handlers"
)

const testSentinelPool = "sentinel"

func TestSentinel_Publish(t *testing.T) {
	query := url.Values{}
	query.Add("delay", "5")
	query.Add("ttl", "10")
	query.Add("tries", "1")
	query.Add("pool", testSentinelPool)
	targetURL := fmt.Sprintf("http://localhost/api/ns/q1?%s", query.Encode())
	body := strings.NewReader("hello msg")
	req, err := http.NewRequest("PUT", targetURL, body)
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

func TestSentinel_Consume(t *testing.T) {
	_, job1 := publishSentinelTestJob("ns", "q2", 0)
	_, job2 := consumeSentinelTestJob("ns", "q2", 1, 1)
	if job1 != job2 {
		t.Fatal("Mismatch job id")
	}
}

func publishSentinelTestJob(ns, q string, delay uint32) (body []byte, jobID string) {
	e := engine.GetEngineByKind("redis", testSentinelPool)
	body = make([]byte, 10)
	if _, err := rand.Read(body); err != nil {
		panic(err)
	}
	jobID, _ = e.Publish(ns, q, body, 60, delay, 1, 0)
	return body, jobID
}

func consumeSentinelTestJob(ns, q string, ttr, timeout uint32) (body []byte, jobID string) {
	e := engine.GetEngineByKind("redis", testSentinelPool)
	job, _ := e.Consume(ns, q, ttr, timeout)
	if job == nil {
		return nil, ""
	}
	return job.Body(), job.ID()
}
