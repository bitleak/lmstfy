package push

import (
	"context"
	"github.com/bitleak/lmstfy/engine"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

func TestPusher(t *testing.T) {
	pusher := newPusher("default", "test-pusher-ns", "test-pusher-queue", &Meta{
		Endpoint: "http://localhost:9090",
		Workers:  5,
		Timeout:  1,
	}, logger)
	if err := pusher.start(); err != nil {
		t.Fatal("Start pusher error", err)
	}
	defer pusher.stop()

	stopPublish := make(chan struct{})
	stopServer := make(chan struct{})
	go func() {
		tick := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-tick.C:
				_, err := engine.GetEngine("default").Publish("test-pusher-ns", "test-pusher-queue", []byte("test-body"), 60, 1, 1)
				if err != nil {
					t.Fatal("Pusher publish error", err)
				}
			case <-stopPublish:
				time.Sleep(time.Second)
				close(stopServer)
				return
			}
		}
	}()
	jobCount := 0
	jobMap := map[string]bool{}
	router := gin.New()
	router.POST("/", func(c *gin.Context) {
		jobMap[c.Request.Header.Get("job_id")] = true
		body, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			t.Fatal("Failed to read request")
		}
		if string(body) == "test-body" {
			jobCount++
			logger.WithFields(logrus.Fields{
				"job_id": c.Request.Header.Get("job_id"),
				"body":   string(body),
				"count":  jobCount,
			}).Info("got job")
		}
		if jobCount == 10 {
			close(stopPublish)
		}
		c.JSON(http.StatusOK, gin.H{"status": "success"})
	})
	server := &http.Server{Addr: "localhost:9090", Handler: router}
	go server.ListenAndServe()

	select {
	case <-stopServer:
		server.Shutdown(context.Background())
	}
	if len(jobMap) != 10 {
		t.Fatal("Mismatch job count")
	}
	size, err := engine.GetEngine("default").Size("test-pusher-ns", "test-pusher-queue")
	if err != nil {
		t.Fatal("Pusher get size error", err)
	}
	if size != 0 {
		t.Fatal("Pusher mismatch size")
	}
}
