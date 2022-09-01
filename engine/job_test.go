package engine

import (
	"bytes"
	"testing"
)

func TestJobImpl_Marshal(t *testing.T) {
	j := NewJob("ns-1", "q-1", []byte("hello data"), 20, 10, 1, "")
	bin, err := j.MarshalBinary()
	if err != nil {
		t.Fatal("Failed to marshal")
	}
	var j2 jobImpl
	err = j2.UnmarshalBinary(bin)
	if err != nil {
		t.Fatalf("Failed to unmarshal: %s", err)
	}
	if j.Namespace() != j2.namespace ||
		j.Queue() != j2.queue ||
		j.TTL() != j2.ttl ||
		j.Delay() != j2.delay ||
		j.Tries() != j2.tries ||
		!bytes.Equal(j.Body(), j2.body) {
		t.Fatal("Data mismatched")
	}
}
