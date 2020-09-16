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
		func(pool, ns, group string, meta *push.Meta) {},
		func(pool, ns, group string, newMeta *push.Meta) {},
		func(pool, ns, group string) {})
}

func TestCreateQueuePusher(t *testing.T) {
	limitStr := "{\"queues\":[\"test-queue\"],\"timeout\": 3, \"workers\": 5, \"endpoint\":\"http://test-endpoint\"}"
	targetUrl := fmt.Sprintf("http://localhost/pusher/ns-pusher/test-group")
	req, err := http.NewRequest(http.MethodPost, targetUrl, strings.NewReader(limitStr))
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	req.Header.Add("Content-type", "application/json")
	c, e, resp := ginTest(req)
	e.POST("/pusher/:namespace/:group", handlers.CreatePushGroup)
	e.HandleContext(c)
	if resp.Code != http.StatusCreated {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to create new push group")
	}
	time.Sleep(500 * time.Millisecond)
	meta := push.GetManager().Get("default", "ns-pusher", "test-group")
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
	e.GET("/pusher", handlers.ListPushGroups)
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
	e.GET("/pusher/:namespace", handlers.ListNamespacePushGroups)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to list namespace pusher")
	}
}

func TestGetPushGroup(t *testing.T) {
	targetUrl := "http://localhost/pusher/ns-pusher/test-group"
	req, err := http.NewRequest(http.MethodGet, targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.GET("/pusher/:namespace/:group", handlers.GetPushGroup)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to list namespace pusher")
	}
}

func TestUpdatePushGroup(t *testing.T) {
	limitStr := "{\"timeout\": 1, \"workers\": 3, \"endpoint\":\"http://test-enpoint-new\"}"
	targetUrl := fmt.Sprintf("http://localhost/pusher/ns-pusher/test-group")
	req, err := http.NewRequest(http.MethodPut, targetUrl, strings.NewReader(limitStr))
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	req.Header.Add("Content-type", "application/json")
	c, e, resp := ginTest(req)
	e.PUT("/pusher/:namespace/:group", handlers.UpdatePushGroup)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to update push group")
	}
	time.Sleep(500 * time.Millisecond)
	meta := push.GetManager().Get("default", "ns-pusher", "test-group")
	if meta == nil || meta.Endpoint != "http://test-enpoint-new" || meta.Timeout != 1 || meta.Workers != 3 {
		t.Fatal("Mismatch meta in update push group")
	}
}

func TestDeletePushGrup(t *testing.T) {
	targetUrl := "http://localhost/pusher/ns-pusher/test-group"
	req, err := http.NewRequest(http.MethodDelete, targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.DELETE("/pusher/:namespace/:group", handlers.DeletePushGroup)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to list namespace pusher")
	}
	time.Sleep(500 * time.Millisecond)
	meta := push.GetManager().Get("default", "ns-pusher", "test-group")
	if meta != nil {
		t.Fatal("Mismatch meta in delete push group")
	}
}
