package handlers_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/meitu/lmstfy/auth"
	"github.com/meitu/lmstfy/server/handlers"
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
	token, _ := tk.New("", "ns-token", "to be deleted")

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
