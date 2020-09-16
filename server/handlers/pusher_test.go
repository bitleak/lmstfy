package handlers_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/push"
	"github.com/bitleak/lmstfy/server/handlers"
)

func TestSetup(t *testing.T) {
	push.GetManager().SetCallbacks(
		func(pool, ns, queue string, meta *push.Meta) {},
		func(pool, ns, queue string, newMeta *push.Meta) {},
		func(pool, ns, queue string) {})
}

func TestCreateQueuePusher(t *testing.T) {
	limitStr := "{\"timeout\": 3, \"workers\": 5, \"endpoint\":\"http://test-endpoint\"}"
	targetUrl := fmt.Sprintf("http://localhost/pusher/ns-pusher/queue-pusher")
	req, err := http.NewRequest(http.MethodPost, targetUrl, strings.NewReader(limitStr))
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	req.Header.Add("Content-type", "application/json")
	c, e, resp := ginTest(req)
	e.POST("/pusher/:namespace/:queue", handlers.CreateQueuePusher)
	e.HandleContext(c)
	if resp.Code != http.StatusCreated {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to create new queue pusher")
	}
	time.Sleep(500 * time.Millisecond)
	meta := push.GetManager().Get("default", "ns-pusher", "queue-pusher")
	if meta == nil || meta.Endpoint != "http://test-endpoint" || meta.Timeout != 3 || meta.Workers != 5 {
		t.Fatal("Mismatch meta in create")
	}
}

func TestListPushers(t *testing.T) {
	targetUrl := "http://localhost/pusher"
	req, err := http.NewRequest(http.MethodGet, targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.GET("/pusher", handlers.ListPushers)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to list pusher")
	}
}

func TestListNamespacePushers(t *testing.T) {
	targetUrl := "http://localhost/pusher/ns-pusher"
	req, err := http.NewRequest(http.MethodGet, targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.GET("/pusher/:namespace", handlers.ListNamespacePushers)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to list namespace pusher")
	}
}

func TestGetQueuePusher(t *testing.T) {
	targetUrl := "http://localhost/pusher/ns-pusher/queue-pusher"
	req, err := http.NewRequest(http.MethodGet, targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.GET("/pusher/:namespace/:queue", handlers.GetQueuePusher)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to list namespace pusher")
	}
}

func TestUpdateQueuePusher(t *testing.T) {
	limitStr := "{\"timeout\": 1, \"workers\": 3, \"endpoint\":\"http://test-enpoint-new\"}"
	targetUrl := fmt.Sprintf("http://localhost/pusher/ns-pusher/queue-pusher")
	req, err := http.NewRequest(http.MethodPut, targetUrl, strings.NewReader(limitStr))
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	req.Header.Add("Content-type", "application/json")
	c, e, resp := ginTest(req)
	e.PUT("/pusher/:namespace/:queue", handlers.UpdateQueuePusher)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to update queue pusher")
	}
	time.Sleep(500 * time.Millisecond)
	meta := push.GetManager().Get("default", "ns-pusher", "queue-pusher")
	if meta == nil || meta.Endpoint != "http://test-enpoint-new" || meta.Timeout != 1 || meta.Workers != 3 {
		t.Fatal("Mismatch meta in update queue pusher")
	}
}

func TestDeleteQueuePusher(t *testing.T) {
	targetUrl := "http://localhost/pusher/ns-pusher/queue-pusher"
	req, err := http.NewRequest(http.MethodDelete, targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.DELETE("/pusher/:namespace/:queue", handlers.DeleteQueuePusher)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to list namespace pusher")
	}
	time.Sleep(500 * time.Millisecond)
	meta := push.GetManager().Get("default", "ns-pusher", "queue-pusher")
	if meta != nil {
		t.Fatal("Mismatch meta in delete queue pusher")
	}
}
