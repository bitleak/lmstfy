package redis

import (
	"bytes"
	"fmt"
	"testing"
)








func TestEngine_Peek(t *testing.T) {
	e, err := NewEngine(R.Name, R.Conn)
	if err != nil {
		panic(fmt.Sprintf("Setup engine error: %s", err))
	}
	defer e.Shutdown()
	body := []byte("hello msg 6")
	jobID, err := e.Publish("ns-engine", "q6", body, 10, 0, 1)
	if err != nil {
		t.Fatalf("Failed to publish: %s", err)
	}
	job, err := e.Peek("ns-engine", "q6", "")
	if err != nil {
		t.Fatalf("Failed to peek: %s", err)
	}
	if job.ID() != jobID || !bytes.Equal(job.Body(), body) {
		t.Fatal("Mismatched job")
	}
}


