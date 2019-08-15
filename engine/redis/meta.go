package redis

import (
	"sync"
)

/**
Record meta info passively. meta info includes:
- namespaces list
- queue list of namespace
*/

type MetaManager struct {
	redis   *RedisInstance
	nsCache map[string]bool // namespace => bool
	qCache  map[string]bool // {namespace}+{queue} => bool
	rwmu    sync.RWMutex
}

func NewMetaManager(redis *RedisInstance) *MetaManager {
	m := &MetaManager{
		redis:   redis,
		nsCache: make(map[string]bool),
		qCache:  make(map[string]bool),
	}
	go m.initialize()
	return m
}

func (m *MetaManager) RecordIfNotExist(namespace, queue string) {
	m.rwmu.RLock()
	if m.nsCache[namespace] && m.qCache[join(namespace, queue)] {
		m.rwmu.RUnlock()
		return
	}
	m.rwmu.RUnlock()

	m.rwmu.Lock()
	if m.nsCache[namespace] {
		m.qCache[join(namespace, queue)] = true
		m.rwmu.Unlock()
		m.redis.Conn.HSet(join(MetaPrefix, "ns", namespace), queue, 1)
	} else {
		m.nsCache[namespace] = true
		m.qCache[join(namespace, queue)] = true
		m.rwmu.Unlock()
		m.redis.Conn.HSet(join(MetaPrefix, "ns"), namespace, 1)
		m.redis.Conn.HSet(join(MetaPrefix, "ns", namespace), queue, 1)
	}
}

func (m *MetaManager) Remove(namespace, queue string) {
	m.rwmu.Lock()
	delete(m.nsCache, namespace)
	delete(m.qCache, join(namespace, queue))
	m.rwmu.Unlock()
	m.redis.Conn.HDel(join(MetaPrefix, "ns", namespace), queue)
}

func (m *MetaManager) ListNamespaces() (namespaces []string, err error) {
	val, err := m.redis.Conn.HGetAll(join(MetaPrefix, "ns")).Result()
	if err != nil {
		return nil, err
	}
	for k := range val {
		namespaces = append(namespaces, k)
	}
	return namespaces, nil
}

func (m *MetaManager) ListQueues(namespace string) (queues []string, err error) {
	val, err := m.redis.Conn.HGetAll(join(MetaPrefix, "ns", namespace)).Result()
	if err != nil {
		return nil, err
	}
	for k := range val {
		queues = append(queues, k)
	}
	return queues, nil
}

func (m *MetaManager) initialize() {
	namespaces, _ := m.ListNamespaces()
	for _, n := range namespaces {
		queues, _ := m.ListQueues(n)
		for _, q := range queues {
			m.rwmu.Lock()
			m.nsCache[n] = true
			m.qCache[join(n, q)] = true
			m.rwmu.Unlock()
		}
	}
}

func (m *MetaManager) Dump() map[string][]string {
	data := make(map[string][]string)
	namespaces, _ := m.ListNamespaces()
	for _, n := range namespaces {
		queues, _ := m.ListQueues(n)
		data[n] = queues
	}
	return data
}
