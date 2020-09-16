package push

import (
	"testing"
	"time"
)

func TestSetup(t *testing.T) {
	GetManager().SetCallbacks(
		func(pool, ns, queue string, meta *Meta) {},
		func(pool, ns, queue string, newMeta *Meta) {},
		func(pool, ns, queue string) {})
}

func TestMetaManager_Dump(t *testing.T) {
	testMeta := &Meta{
		Endpoint: "test-endpoint",
		Workers:  5,
		Timeout:  5,
	}
	err := _manager.Create("default", "test-ns-dump1", "test-queue1", testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	err = _manager.Create("default", "test-ns-dump2", "test-queue2", testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	metaMap := _manager.Dump()
	if len(metaMap) != 1 {
		t.Fatalf("expect dump return 1 metaMaps, but got %d", len(metaMap))
	}
	for pool, poolMetaMap := range metaMap {
		if len(poolMetaMap) != 2 {
			t.Fatalf("expect dump return 2 poolMetaMap, but got %d", len(poolMetaMap))
		}
		if pool != "default" {
			t.Fatalf("expect dump metaMap contains default, but got %s", pool)
		}
		for ns, nsMetas := range poolMetaMap {
			if ns != "test-ns-dump1" && ns != "test-ns-dump2" {
				t.Fatalf("expect dump metaMap[default] contains ns %s or %s , but got %s", "test-ns-dump1", "test-ns-dump2", ns)
			}
			if len(nsMetas) != 1 {
				t.Fatalf("expect dump metaMap[default][pool] contains 1 queue, but got %d", len(nsMetas))
			}
			if nsMetas[0] != "test-queue1" && nsMetas[0] != "test-queue2" {
				t.Fatalf("expect dump metaMap[default][pool] contains queue %s or %s , but got %s", "test-queue1", "test-queue2", nsMetas[0])
			}
		}
	}
}

func TestMetaManager_CreateAndGet(t *testing.T) {
	testMeta := &Meta{
		Endpoint: "test-endpoint",
		Workers:  5,
		Timeout:  5,
	}
	err := _manager.Create("default", "test-ns", "test-queue-get", testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	meta, err := _manager.GetFromRemote("default", "test-ns", "test-queue-get")
	if err != nil {
		t.Fatalf("expect metaManager get from remote return nil, but got %v", err)
	}
	if meta.Timeout != testMeta.Timeout || meta.Workers != testMeta.Workers || meta.Endpoint != testMeta.Endpoint {
		t.Fatalf("expect metaManager get from remote meta equel testMeta")
	}
	time.Sleep(500 * time.Millisecond)
	meta = _manager.Get("default", "test-ns", "test-queue-get")
	if meta == nil {
		t.Fatalf("expect metaManager get return meta, but got nil")
	}
	if meta.Timeout != testMeta.Timeout || meta.Workers != testMeta.Workers || meta.Endpoint != testMeta.Endpoint {
		t.Fatalf("expect metaManager get meta equel testMeta")
	}
}

func TestMetaManager_Update(t *testing.T) {
	testMeta := &Meta{
		Endpoint: "test-endpoint",
		Workers:  5,
		Timeout:  5,
	}
	err := _manager.Create("default", "test-ns", "test-queue-update", testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	newMeta := &Meta{
		Endpoint: "test-endpoint-new",
		Workers:  10,
		Timeout:  10,
	}
	err = _manager.Update("default", "test-ns", "test-queue-update", newMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	meta, err := _manager.GetFromRemote("default", "test-ns", "test-queue-update")
	if err != nil {
		t.Fatalf("expect metaManager get from remote return nil, but got %v", err)
	}
	if meta.Timeout != newMeta.Timeout || meta.Workers != newMeta.Workers || meta.Endpoint != newMeta.Endpoint {
		t.Fatalf("expect metaManager get from remote meta equel newMeta")
	}
	// wait for meta update and pusher restart
	time.Sleep(500 * time.Millisecond)
	meta = _manager.Get("default", "test-ns", "test-queue-update")
	if meta == nil {
		t.Fatalf("expect metaManager get return meta, but got nil")
	}
	if meta.Timeout != newMeta.Timeout || meta.Workers != newMeta.Workers || meta.Endpoint != newMeta.Endpoint {
		t.Fatalf("expect metaManager get meta equel newMeta")
	}
}

func TestMetaManager_Delete(t *testing.T) {
	testMeta := &Meta{
		Endpoint: "test-endpoint",
		Workers:  5,
		Timeout:  5,
	}
	err := _manager.Create("default", "test-ns", "test-queue-delete", testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	meta := _manager.Get("default", "test-ns", "test-queue-delete")
	if meta == nil {
		t.Fatalf("expect metaManager get return meta, but got nil")
	}
	if meta.Timeout != testMeta.Timeout || meta.Workers != testMeta.Workers || meta.Endpoint != testMeta.Endpoint {
		t.Fatalf("expect metaManager get meta equel testMeta")
	}

	err = _manager.MetaManager.Delete("default", "test-ns", "test-queue-delete")
	if err != nil {
		t.Fatalf("expect metaManager delete return nil, but got %v", err)
	}
	meta, err = _manager.GetFromRemote("default", "test-ns", "test-queue-delete")
	if err != nil {
		t.Fatalf("expect metaManager get from remote return nil, but got %v", err)
	}
	if meta != nil {
		t.Fatalf("expect metaManager get from remote meta equel nil")
	}
	// wait for meta update and pusher stop
	time.Sleep(500 * time.Millisecond)
	meta = _manager.Get("default", "test-ns", "test-queue-delete")
	if meta != nil {
		t.Fatalf("expect metaManager get return nil")
	}
}

func TestMetaManager_ListPusherByNamespace(t *testing.T) {
	testMeta := &Meta{
		Endpoint: "test-endpoint",
		Workers:  5,
		Timeout:  5,
	}
	err := _manager.Create("default", "test-ns-list", "test-queue1", testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	err = _manager.Create("default", "test-ns-list", "test-queue2", testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	metaMap := _manager.ListPusherByNamespace("default", "test-ns-list")
	if len(metaMap) != 2 {
		t.Fatalf("expect list pusher by namespace return 2 meta, but got %d", len(metaMap))
	}
	for queue, meta := range metaMap {
		if queue != "test-queue1" && queue != "test-queue2" {
			t.Fatalf("expect list pusher by namespace return queue %s or %s , but got %s", "test-queue1", "test-queue2", queue)
		}
		if meta.Timeout != testMeta.Timeout || meta.Workers != testMeta.Workers || meta.Endpoint != testMeta.Endpoint {
			t.Fatalf("expect list pusher by namespace return meta equel testMeta")
		}
	}

}

func TestShutdown(t *testing.T) {
	GetManager().SetCallbacks(_manager.onCreated, _manager.onUpdated, _manager.onDeleted)
}
