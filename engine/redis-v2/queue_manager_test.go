package redis_v2

import (
	"testing"
	"time"
)

func TestQueueManager_AddAndRemove(t *testing.T) {
	queueManager, err := NewQueueManager(R)
	if err != nil {
		t.Fatal("init queue manager error", err)
		return
	}
	defer queueManager.Close()

	err = queueManager.Add("test-ns", "test-q")
	if err != nil {
		t.Fatal("unexpect add queuedata error", err)
		return
	}
	time.Sleep(1 * time.Second)
	if !queueManager.Exist("test-ns", "test-q") {
		t.Fatal("expect queue exist, but got false")
		return
	}

	err = queueManager.Remove("test-ns", "test-q")
	if err != nil {
		t.Fatal("unexpect remove queue data error", err)
		return
	}
	time.Sleep(1 * time.Second)
	if queueManager.Exist("test-ns", "test-q") {
		t.Fatal("expect queue not exist, but got true")
		return
	}
}

func TestQueueManager_Exist(t *testing.T) {
	queueManager, err := NewQueueManager(R)
	if err != nil {
		t.Fatal("init queue manager error", err)
		return
	}
	defer queueManager.Close()
	err = queueManager.Add("test-ns1", "test-q1")
	if err != nil {
		t.Fatal("unexpect add queue data error", err)
		return
	}
	time.Sleep(1 * time.Second)
	if !queueManager.Exist("test-ns1", "test-q1") {
		t.Fatal("expect queue exist, but got false")
		return
	}
	if queueManager.Exist("test-ns-notexist", "test-q1") {
		t.Fatal("expect queue not exist, but got true")
		return
	}
	if queueManager.Exist("test-ns1", "test-q1-notexist") {
		t.Fatal("expect queue not exist, but got true")
		return
	}
}

func TestQueueManager_Dump(t *testing.T) {
	queueManager, err := NewQueueManager(R)
	if err != nil {
		t.Fatal("init queue manager error", err)
		return
	}
	defer queueManager.Close()
	ns := "test-ns2"
	queues := []string{"test-q1", "test-q2"}
	for _, q := range queues {
		err = queueManager.Add(ns, q)
		if err != nil {
			t.Fatal("unexpect add queue data error", err)
			return
		}
	}

	time.Sleep(1 * time.Second)
	data, err := queueManager.Dump()
	if err != nil {
		t.Fatal("unexpect dump queuedata error", err)
		return
	}
	if _, ok := data[ns]; !ok {
		t.Fatalf("expect dump info contains ns %s, but not exist", ns)
		return
	}
	if len(data[ns]) != 2 {
		t.Fatalf("expect dump info ns %s contains 2 queue, but got %d", ns, len(data[ns]))
		return
	}
	for _, q := range data[ns] {
		if q != queues[0] && q != queues[1] {
			t.Fatalf("unexpect queue %s in ns %s", q, ns)
			return
		}
	}
}
