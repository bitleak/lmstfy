package push

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/engine"
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
	namespace := "test-pusher-ns"
	queue := "test-pusher-queue"
	pusher := newPusher("default", "test-pusher-ns", "test-pusher-group", &Meta{
		Queues:   []string{"test-pusher-queue"},
		Endpoint: "http://localhost:9090",
		Workers:  5,
		Timeout:  3,
	}, logger)
	if err := pusher.start(); err != nil {
		t.Fatal("Start pusher error", err)
	}
	defer pusher.stop()

	stopPublish := make(chan struct{})
	stopServer := make(chan struct{})
	sentJobs := make(map[string]bool)
	for i := 0; i < 10; i++ {
		jobID, err := engine.GetEngine(defaultPool).Publish(namespace, queue, []byte("test-body"), 60, 1, 1)
		if err != nil {
			t.Fatal("Pusher publish error", err)
		}
		sentJobs[jobID] = true
	}
	go func() {
		tick := time.NewTicker(time.Second)
		for {
			select {
			case <-tick.C:
				jobID, err := engine.GetEngine(defaultPool).Publish(namespace, queue, []byte("test-body"), 60, 1, 1)
				if err != nil {
					t.Fatal("Pusher publish error", err)
				}
				sentJobs[jobID] = true
			case <-stopPublish:
				time.Sleep(time.Second)
				close(stopServer)
				return
			}
		}
	}()
	jobCount := 0
	gotJobs := map[string]bool{}
	router := gin.New()
	router.POST("/", func(c *gin.Context) {
		var job Job
		bytes, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			t.Fatal("Failed to read request")
		}
		_ = json.Unmarshal(bytes, &job)
		gotJobs[job.ID] = true
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
	if len(gotJobs) != len(sentJobs) {
		t.Fatalf("Mismatch job count, expected: %d, but got %d", len(sentJobs), len(gotJobs))
	}
	size, err := engine.GetEngine(defaultPool).Size(namespace, queue)
	if err != nil {
		t.Fatal("Pusher get size error", err)
	}
	if size != 0 {
		t.Fatal("Pusher mismatch size")
	}
}
