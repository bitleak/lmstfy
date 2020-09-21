package handlers_test

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/auth"
	"github.com/bitleak/lmstfy/server/handlers"
	throttler2 "github.com/bitleak/lmstfy/throttler"
	"github.com/bitleak/lmstfy/uuid"
)

func publishWithThrottler(namespace, queue, token string) error {
	query := url.Values{}
	query.Add("delay", "0")
	query.Add("ttl", "10")
	query.Add("tries", "100")
	query.Add("token", token)
	targetURL := fmt.Sprintf("http://localhost/api/%s/%s?%s", namespace, queue, query.Encode())
	body := strings.NewReader("hello msg")
	req, err := http.NewRequest("PUT", targetURL, body)
	if err != nil {
		return fmt.Errorf("create request err: %s", err.Error())
	}
	c, e, resp := ginTest(req)
	throttler := throttler2.GetThrottler()
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine, handlers.Throttle(throttler, "produce"))
	e.PUT("/api/:namespace/:queue", handlers.Publish)
	e.HandleContext(c)
	if resp.Code != http.StatusCreated {
		return fmt.Errorf("publish code: %d, error: %s", resp.Code, resp.Body.String())
	}
	return nil
}

func consumeWithThrottler(namespace, queue, token string) error {
	query := url.Values{}
	query.Add("ttr", "10")
	query.Add("timeout", "2")
	query.Add("token", token)
	targetURL := fmt.Sprintf("http://localhost/api/%s/%s?%s", namespace, queue, query.Encode())
	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		return fmt.Errorf("create request err: %s", err.Error())
	}
	c, e, resp := ginTest(req)
	throttler := throttler2.GetThrottler()
	e.Use(handlers.ValidateParams, handlers.SetupQueueEngine, handlers.Throttle(throttler, "consume"))
	e.GET("/api/:namespace/:queue", handlers.Consume)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		return fmt.Errorf("consume code: %d, error: %s", resp.Code, resp.Body.String())
	}
	return nil
}

func TestThrottlePublish(t *testing.T) {
	tk := auth.GetTokenManager()
	namespace := "ns-throttler"
	queue := "q1"
	token, _ := tk.New("", namespace, uuid.GenUniqueID(), "test publish throttler")
	limitStr := "{\"read\": 0, \"write\": 3, \"interval\":1}"
	if err := addTokenLimit(namespace, token, limitStr); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		err := publishWithThrottler(namespace, queue, token)
		if i != 3 {
			if err != nil {
				t.Fatalf("Publish the test job, err: %s", err.Error())
			}
		} else {
			if err == nil {
				t.Fatalf("Publish test job reached limit error was expected")
			}
		}
	}
	time.Sleep(2 * time.Second)
	// retry after interval
	err := publishWithThrottler(namespace, queue, token)
	if err != nil {
		t.Fatalf("Publish the test job, err: %s", err.Error())
	}
}

func TestThrottleConsume(t *testing.T) {
	tk := auth.GetTokenManager()
	namespace := "ns-throttler"
	queue := "q2"
	token, _ := tk.New("", namespace, uuid.GenUniqueID(), "test publish throttler")
	limitStr := "{\"read\": 3, \"write\": 100, \"interval\":1}"
	if err := addTokenLimit(namespace, token, limitStr); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		err := publishWithThrottler(namespace, queue, token)
		if err != nil {
			t.Fatalf("Publish test job, err: %s", err.Error())
		}
	}
	for i := 0; i < 4; i++ {
		err := consumeWithThrottler(namespace, queue, token)
		if i != 3 {
			if err != nil {
				t.Fatalf("Consume the test job, err: %s", err.Error())
			}
		} else {
			if err == nil {
				t.Fatalf("Consume test job reached limit error was expected")
			}
		}
	}
	// retry after interval
	time.Sleep(2 * time.Second)
	err := consumeWithThrottler(namespace, queue, token)
	if err != nil {
		t.Fatalf("Consume the test job, err: %s", err.Error())
	}
}
