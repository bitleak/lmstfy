package push

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const defaultPool = "default"
const defaultNamespace = "test-ns"

func TestSetup(t *testing.T) {
	GetManager().SetCallbacks(
		func(pool, ns, group string, meta *Meta) {},
		func(pool, ns, group string, newMeta *Meta) {},
		func(pool, ns, group string) {})
}

func TestMetaManager_Dump(t *testing.T) {
	testMeta := &Meta{
		Queues:   []string{"abc"},
		Endpoint: "test-endpoint",
		Workers:  5,
		Timeout:  5,
	}
	namespace := "test-ns-dump"
	groups := []string{"test-group1", "test-group2"}
	for _, group := range groups {
		err := _manager.Create(defaultPool, namespace, group, testMeta)
		if err != nil {
			t.Fatalf("create group expect error is nil, but got %v", err)
		}
	}
	time.Sleep(500 * time.Millisecond)
	poolMetas := _manager.Dump()
	if len(poolMetas) != 1 {
		t.Fatalf("expect dump return 1 metaMaps, but got %d", len(poolMetas))
	}
	for poolName, poolMeta := range poolMetas {
		if len(poolMeta) != 1 {
			t.Fatalf("the number of the metas %d was expected, but got %d", 1, len(poolMeta))
		}
		if poolName != defaultPool {
			t.Fatalf("pool name %s was expected, but got %s", defaultPool, poolName)
		}
		for ns, gotGroups := range poolMeta {
			if ns != namespace {
				t.Fatalf("namespace %s was expected, but got %s", namespace, ns)
			}
			if len(gotGroups) != len(groups) {
				t.Fatalf("the number of group %d was expected, but got %d", len(groups), len(gotGroups))
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
	namespace := "test-ns"
	group := "test-get-group"
	err := _manager.Create(defaultPool, namespace, group, testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	meta, err := _manager.GetFromRemote(defaultPool, defaultNamespace, group)
	if err != nil {
		t.Fatalf("expect metaManager get from remote return nil, but got %v", err)
	}
	if !cmp.Equal(meta, testMeta) {
		t.Fatalf("expect metaManager get from remote meta equel testMeta")
	}
	time.Sleep(500 * time.Millisecond)
	meta = _manager.Get(defaultPool, defaultNamespace, group)
	if meta == nil {
		t.Fatalf("expect metaManager get return meta, but got nil")
	}
	if !cmp.Equal(meta, testMeta) {
		t.Fatalf("expect metaManager get meta equel testMeta")
	}
}

func TestMetaManager_Update(t *testing.T) {
	testMeta := &Meta{
		Endpoint: "test-endpoint",
		Workers:  5,
		Timeout:  5,
	}
	group := "test-group-update"
	err := _manager.Create(defaultPool, defaultNamespace, group, testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	newMeta := &Meta{
		Endpoint: "test-endpoint-new",
		Workers:  10,
		Timeout:  10,
	}
	err = _manager.Update(defaultPool, defaultNamespace, group, newMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	meta, err := _manager.GetFromRemote(defaultPool, defaultNamespace, group)
	if err != nil {
		t.Fatalf("expect metaManager get from remote return nil, but got %v", err)
	}
	if !cmp.Equal(meta, newMeta) {
		t.Fatalf("expect metaManager get from remote meta equel testMeta")
	}
	// wait for meta update and pusher restart
	time.Sleep(500 * time.Millisecond)
	meta = _manager.Get(defaultPool, defaultNamespace, group)
	if meta == nil {
		t.Fatalf("expect metaManager get return meta, but got nil")
	}
	if !cmp.Equal(meta, newMeta) {
		t.Fatalf("expect metaManager get meta equel testMeta")
	}
}

func TestMetaManager_Delete(t *testing.T) {
	testMeta := &Meta{
		Endpoint: "test-endpoint",
		Workers:  5,
		Timeout:  5,
	}
	group := "test-group-delete"
	err := _manager.Create(defaultPool, defaultNamespace, group, testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	meta := _manager.Get(defaultPool, defaultNamespace, group)
	if meta == nil {
		t.Fatalf("expect metaManager get return meta, but got nil")
	}
	if !cmp.Equal(meta, testMeta) {
		t.Fatalf("expect metaManager get meta equel testMeta")
	}

	err = _manager.MetaManager.Delete(defaultPool, defaultNamespace, group)
	if err != nil {
		t.Fatalf("expect metaManager delete return nil, but got %v", err)
	}
	meta, err = _manager.GetFromRemote(defaultPool, defaultNamespace, group)
	if err != nil {
		t.Fatalf("expect metaManager get from remote return nil, but got %v", err)
	}
	if meta != nil {
		t.Fatalf("expect metaManager get from remote meta equel nil")
	}
	// wait for meta update and pusher stop
	time.Sleep(500 * time.Millisecond)
	meta = _manager.Get(defaultPool, defaultNamespace, group)
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
	namespace := "test-ns-list"
	err := _manager.Create(defaultPool, namespace, "test-group1", testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	err = _manager.Create(defaultPool, namespace, "test-group2", testMeta)
	if err != nil {
		t.Fatalf("expect metaManager create return nil, but got %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	metaMap := _manager.ListPusherByNamespace(defaultPool, namespace)
	if len(metaMap) != 2 {
		t.Fatalf("expect list pusher by namespace return 2 meta, but got %d", len(metaMap))
	}
	for queue, meta := range metaMap {
		if queue != "test-group1" && queue != "test-group2" {
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
