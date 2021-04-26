package redis_v2

import (
	"testing"
	"time"
)

func TestMetaManager_AddAndRemove(t *testing.T) {
	metaManager, err := NewMetaManager(R)
	if err != nil {
		t.Fatal("init meta manager error", err)
		return
	}
	defer metaManager.Close()

	err = metaManager.Add("test-ns", "test-q")
	if err != nil {
		t.Fatal("unexpect add metadata error", err)
		return
	}
	time.Sleep(1 * time.Second)
	if !metaManager.Exist("test-ns", "test-q") {
		t.Fatal("expect meta exist, but got false")
		return
	}

	err = metaManager.Remove("test-ns", "test-q")
	if err != nil {
		t.Fatal("unexpect remove metadata error", err)
		return
	}
	time.Sleep(1 * time.Second)
	if metaManager.Exist("test-ns", "test-q") {
		t.Fatal("expect meta not exist, but got true")
		return
	}
}

func TestMetaManager_Exist(t *testing.T) {
	metaManager, err := NewMetaManager(R)
	if err != nil {
		t.Fatal("init meta manager error", err)
		return
	}
	defer metaManager.Close()
	err = metaManager.Add("test-ns1", "test-q1")
	if err != nil {
		t.Fatal("unexpect add metadata error", err)
		return
	}
	time.Sleep(1 * time.Second)
	if !metaManager.Exist("test-ns1", "test-q1") {
		t.Fatal("expect meta exist, but got false")
		return
	}
	if metaManager.Exist("test-ns-notexist", "test-q1") {
		t.Fatal("expect meta not exist, but got true")
		return
	}
	if metaManager.Exist("test-ns1", "test-q1-notexist") {
		t.Fatal("expect meta not exist, but got true")
		return
	}
}

func TestMetaManager_Dump(t *testing.T) {
	metaManager, err := NewMetaManager(R)
	if err != nil {
		t.Fatal("init meta manager error", err)
		return
	}
	defer metaManager.Close()
	ns := "test-ns2"
	queues := []string{"test-q1", "test-q2"}
	for _, q := range queues {
		err = metaManager.Add(ns, q)
		if err != nil {
			t.Fatal("unexpect add metadata error", err)
			return
		}
	}

	time.Sleep(1 * time.Second)
	data, err := metaManager.Dump()
	if err != nil {
		t.Fatal("unexpect dump metadata error", err)
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
