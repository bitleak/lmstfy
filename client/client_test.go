package client

import (
	"testing"
	"time"
)

func TestLmstfyClient_Publish(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	cli.ConfigRetry(3, 5000)
	jobID, err := cli.Publish("test-publish", []byte("hello"), 0, 1, 0)
	if err != nil {
		t.Fatalf("Failed to send job: %s", err)
	}
	if jobID == "" {
		t.Fatal("Expected jobID")
	}
}

func TestLmstfyClient_Consume(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-consume", []byte("hello"), 0, 1, 0)
	job, err := cli.Consume("test-consume", 10, 0)
	if err != nil {
		t.Fatalf("Failed to fetch job: %s", err)
	}
	if jobID != job.ID || string(job.Data) != "hello" {
		t.Fatal("Mismatched data")
	}
}

func TestLmstfyClient_BatchConsume(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobMap := map[string]bool{}
	for i := 0; i < 4; i++ {
		jobID, _ := cli.Publish("test-batchconsume", []byte("hello"), 0, 1, 0)
		jobMap[jobID] = true
	}

	jobs, err := cli.BatchConsume("test-batchconsume", 3, 3, 3)
	if err != nil {
		t.Fatalf("Failed to fetch job: %s", err)
	}
	if len(jobs) != 3 {
		t.Fatal("Mismatched job count")
	}
	for _, job := range jobs {
		if !jobMap[job.ID] || string(job.Data) != "hello" {
			t.Fatal("Mismatched data")
		}
	}

	jobs, err = cli.BatchConsume("test-batchconsume", 3, 3, 3)
	if err != nil {
		t.Fatalf("Failed to fetch job: %s", err)
	}
	if len(jobs) != 1 {
		t.Fatal("Mismatched job count")
	}
	for _, job := range jobs {
		if !jobMap[job.ID] || string(job.Data) != "hello" {
			t.Fatal("Mismatched data")
		}
	}

	now := time.Now()
	jobs, err = cli.BatchConsume("test-batchconsume", 3, 3, 3)
	if err != nil {
		t.Fatalf("Failed to fetch job: %s", err)
	}
	if len(jobs) != 0 {
		t.Fatal("Mismatched job count")
	}
	if time.Now().Sub(now) < 3*time.Second {
		t.Fatal("Mismatched timeout second")
	}
}

func TestLmstfyClient_Ack(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	cli.Publish("test-ack", []byte("hello"), 0, 1, 0)
	job, _ := cli.Consume("test-ack", 10, 0)
	err := cli.Ack("test-finish", job.ID)
	if err != nil {
		t.Fatalf("Failed to finish a job: %s", err)
	}
}

func TestLmstfyClient_ConsumeFromQueues(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	cli.Publish("test-multi-consume1", []byte("hello1"), 0, 1, 0)
	jobID, _ := cli.Publish("test-multi-consume2", []byte("hello2"), 0, 1, 0)
	job, err := cli.ConsumeFromQueues(10, 1, "test-multi-consume2", "test-multi-consume1")
	if err != nil {
		t.Fatalf("Failed to fetch job: %s", err)
	}
	if job.Queue != "test-multi-consume2" || jobID != job.ID || string(job.Data) != "hello2" {
		t.Fatal("Mismatched data")
	}
}

func TestLmstfyClient_QueueSize(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	cli.Publish("test-queue-size", []byte("hello"), 0, 1, 0)
	cli.Publish("test-queue-size", []byte("hello"), 0, 1, 0)
	size, err := cli.QueueSize("test-queue-size")
	if err != nil {
		t.Fatalf("Failed to get queue size: %s", err)
	}
	if size != 2 {
		t.Fatal("Expected queue size == 2")
	}
}

func TestLmstfyClient_PeekQueue(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-peek-queue", []byte("hello1"), 0, 1, 0)
	cli.Publish("test-peek-queue", []byte("hello2"), 0, 1, 0)
	job, err := cli.PeekQueue("test-peek-queue")
	if err != nil {
		t.Fatalf("Failed to peek queue: %s", err)
	}
	if jobID != job.ID || string(job.Data) != "hello1" {
		t.Fatal("Mismatched data")
	}

	size, _ := cli.QueueSize("test-peek-queue")
	if size != 2 {
		t.Fatal("Expected queue size == 2")
	}
}

func TestLmstfyClient_PeekJob(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-peek-job", []byte("hello1"), 0, 1, 0)
	cli.Publish("test-peek-job", []byte("hello2"), 0, 1, 0)
	job, err := cli.PeekJob("test-peek-job", jobID)
	if err != nil {
		t.Fatalf("Failed to peek job: %s", err)
	}
	if string(job.Data) != "hello1" {
		t.Fatal("Mismatched data")
	}
}

func TestLmstfyClient_PeekDeadLetter(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-peek-deadletter", []byte("hello1"), 0, 1, 0)
	cli.Consume("test-peek-deadletter", 1, 0)
	time.Sleep(2 * time.Second) // wait til TTR expires
	size, head, err := cli.PeekDeadLetter("test-peek-deadletter")
	if err != nil {
		t.Fatalf("Failed to peek dead letter: %s", err)
	}
	if size != 1 && head != jobID {
		t.Fatal("Mismatched data")
	}
}

func TestLmstfyClient_RespawnDeadLetter(t *testing.T) {
	cli := NewLmstfyClient(Host, Port, Namespace, Token)
	jobID, _ := cli.Publish("test-respawn-deadletter", []byte("hello1"), 0, 1, 0)
	cli.Consume("test-respawn-deadletter", 1, 0)
	time.Sleep(2 * time.Second) // wait til TTR expires
	count, err := cli.RespawnDeadLetter("test-respawn-deadletter", 2, 120)
	if err != nil {
		t.Fatalf("Failed to respawn deadletter: %s", err)
	}
	if count != 1 {
		t.Fatal("Mismatched deadletter size")
	}
	job, _ := cli.Consume("test-respawn-deadletter", 1, 0)
	if string(job.Data) != "hello1" || jobID != job.ID {
		t.Fatal("Mismatched data")
	}
}
