package push

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/bitleak/lmstfy/engine"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type Job struct {
	Namespace string `json:"namespace"`
	Queue     string `json:"queue"`
	ID        string `json:"id"`
	TTL       uint32 `json:"ttl"`
	ElapsedMS int64  `json:"elapsed_ms"`
	Body      []byte `json:"body"`
}

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
		var job Job
		bytes, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			t.Fatal("Failed to read request")
		}
		_ = json.Unmarshal(bytes, &job)
		jobMap[job.ID] = true
		if string(job.Body) == "test-body" {
			jobCount++
			logger.WithFields(logrus.Fields{
				"id":    job.ID,
				"body":  string(job.Body),
				"count": jobCount,
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
