package handlers_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/bitleak/lmstfy/auth"
	"github.com/bitleak/lmstfy/server/handlers"
	"github.com/bitleak/lmstfy/throttler"
	"github.com/bitleak/lmstfy/uuid"
)

func TestNewToken(t *testing.T) {
	query := url.Values{}
	query.Add("description", "test")
	targetUrl := fmt.Sprintf("http://localhost/token/ns-token?%s", query.Encode())
	req, err := http.NewRequest("POST", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.POST("/token/:namespace", handlers.NewToken)
	e.HandleContext(c)
	if resp.Code != http.StatusCreated {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to create new token")
	}
}

func TestListTokens(t *testing.T) {
	targetUrl := "http://localhost/token/ns-token"
	req, err := http.NewRequest("GET", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.GET("/token/:namespace", handlers.ListTokens)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to list tokens")
	}
}

func TestDeleteToken(t *testing.T) {
	tk := auth.GetTokenManager()
	token, _ := tk.New("", "ns-token", uuid.GenUniqueID(), "to be deleted")

	targetUrl := fmt.Sprintf("http://localhost/token/ns-token/%s", token)
	req, err := http.NewRequest("DELETE", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.DELETE("/token/:namespace/:token", handlers.DeleteToken)
	e.HandleContext(c)
	if resp.Code != http.StatusNoContent {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to delete token")
	}

	ok, _ := tk.Exist("", "ns-token", token)
	if ok {
		t.Fatal("Expected token to be deleted")
	}
}

func getLimiter(namespace, token string) (*throttler.Limiter, error) {
	// force update limiters
	throttler.GetThrottler().GetAll(true)
	targetUrl := fmt.Sprintf("http://localhost/token/%s/%s/limit", namespace, token)
	req, err := http.NewRequest("GET", targetUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create the request, err: %s", err.Error())
	}
	c, e, resp := ginTest(req)
	e.GET("/token/:namespace/:token/limit", handlers.GetLimiter)
	e.HandleContext(c)
	if resp.Code != http.StatusOK && resp.Code != http.StatusNotFound {
		return nil, fmt.Errorf("http code expected  %d, but got %d", http.StatusOK, resp.Code)
	}
	if resp.Code == http.StatusOK {
		var limiter throttler.Limiter
		json.Unmarshal(resp.Body.Bytes(), &limiter)
		return &limiter, nil
	}
	return nil, nil
}

func addTokenLimit(namespace, token, limitStr string) error {
	targetUrl := fmt.Sprintf("http://localhost/token/%s/%s/limit", namespace, token)
	req, err := http.NewRequest("POST", targetUrl, bytes.NewReader([]byte(limitStr)))
	if err != nil {
		return fmt.Errorf("failed to create the request, err: %s", err.Error())
	}
	c, e, resp := ginTest(req)
	e.POST("/token/:namespace/:token/limit", handlers.AddLimiter)
	e.HandleContext(c)
	if resp.Code != http.StatusCreated {
		return fmt.Errorf("http code expected  %d, but got %d", http.StatusOK, resp.Code)
	}
	return nil
}

func TestAddTokenLimiter(t *testing.T) {
	limitStr := "{\"read\": 100, \"write\": 100, \"interval\":100}"
	namespace := "ns-token"
	tk := auth.GetTokenManager()
	token, _ := tk.New("", "ns-token", uuid.GenUniqueID(), "token limiter")
	if err := addTokenLimit(namespace, token, limitStr); err != nil {
		t.Fatal(err)
	}
	limiter, err := getLimiter(namespace, token)
	if err != nil {
		t.Fatal(err.Error())
	}
	if limiter.Read != 100 && limiter.Write != 100 && limiter.Interval != 100 {
		t.Fatalf("Invaild limiter's value, %v", limiter)
	}
	if err := addTokenLimit(namespace, token, limitStr); err == nil {
		t.Fatal("duplicate token error was expected")
	}
}

func TestSetTokenLimiter(t *testing.T) {
	tk := auth.GetTokenManager()
	token, _ := tk.New("", "ns-token", uuid.GenUniqueID(), "token limiter")
	targetUrl := fmt.Sprintf("http://localhost/token/ns-token/%s/limit", token)
	limitStr := "{\"read\": 100, \"write\": 100, \"interval\":100}"
	req, err := http.NewRequest("PUT", targetUrl, bytes.NewReader([]byte(limitStr)))
	if err != nil {
		t.Fatalf("Failed to create the request, err: %s", err.Error())
	}
	c, e, resp := ginTest(req)
	e.PUT("/token/:namespace/:token/limit", handlers.SetLimiter)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatalf("Failed to add the limit to the token, err: %v", err)
	}
}

func TestDeleteTokenLimiter(t *testing.T) {
	limitStr := "{\"read\": 100, \"write\": 100, \"interval\":100}"
	namespace := "ns-token"
	tk := auth.GetTokenManager()
	token, _ := tk.New("", "ns-token", uuid.GenUniqueID(), "token limiter")
	if err := addTokenLimit(namespace, token, limitStr); err != nil {
		t.Fatal(err)
	}

	targetUrl := fmt.Sprintf("http://localhost/token/%s/%s/limit", namespace, token)
	req, err := http.NewRequest("DELETE", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create the request, err: %s", err.Error())
	}
	c, e, resp := ginTest(req)
	e.DELETE("/token/:namespace/:token/limit", handlers.DeleteLimiter)
	e.HandleContext(c)
	if resp.Code != http.StatusOK {
		t.Logf(resp.Body.String())
		t.Fatalf("Failed to add the limit to the token, err: %v", err)
	}

	limiter, err := getLimiter("ns-token", token)
	if err != nil {
		t.Fatal(err.Error())
	}
	if limiter != nil {
		t.Fatal("the token's limiter was expected to be deleted")
	}
}

func TestRegisterQueue(t *testing.T) {
	query := url.Values{}
	query.Add("pool", "test_v2")
	targetUrl := fmt.Sprintf("http://localhost/queue/test_ns/test_queue?%s", query.Encode())
	req, err := http.NewRequest("POST", targetUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create request")
	}
	c, e, resp := ginTest(req)
	e.POST("/queue/:namespace/:queue", handlers.ValidateParams, handlers.CheckPoolExists, handlers.RegisterQueue)
	e.HandleContext(c)
	if resp.Code != http.StatusCreated {
		t.Logf(resp.Body.String())
		t.Fatal("Failed to register queue")
	}
}
