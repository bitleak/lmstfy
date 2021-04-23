package redis

import (
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/engine"
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

func (m *MetaManager) RecordIfNotExist(meta engine.QueueMeta) {
	m.rwmu.RLock()
	if m.nsCache[meta.Namespace] && m.qCache[join(meta.Namespace, meta.Queue)] {
		m.rwmu.RUnlock()
		return
	}
	m.rwmu.RUnlock()

	m.rwmu.Lock()
	if m.nsCache[meta.Namespace] {
		m.qCache[join(meta.Namespace, meta.Queue)] = true
		m.rwmu.Unlock()
		m.redis.Conn.HSet(dummyCtx, join(MetaPrefix, "ns", meta.Namespace), meta.Queue, 1)
	} else {
		m.nsCache[meta.Namespace] = true
		m.qCache[join(meta.Namespace, meta.Queue)] = true
		m.rwmu.Unlock()
		m.redis.Conn.HSet(dummyCtx, join(MetaPrefix, "ns"), meta.Namespace, 1)
		m.redis.Conn.HSet(dummyCtx, join(MetaPrefix, "ns", meta.Namespace), meta.Queue, 1)
	}
}

func (m *MetaManager) Remove(meta engine.QueueMeta) {
	m.rwmu.Lock()
	delete(m.nsCache, meta.Namespace)
	delete(m.qCache, join(meta.Namespace, meta.Queue))
	m.rwmu.Unlock()
	m.redis.Conn.HDel(dummyCtx, join(MetaPrefix, "ns", meta.Namespace), meta.Queue)
}

func (m *MetaManager) ListNamespaces() (namespaces []string, err error) {
	val, err := m.redis.Conn.HGetAll(dummyCtx, join(MetaPrefix, "ns")).Result()
	if err != nil {
		return nil, err
	}
	for k := range val {
		namespaces = append(namespaces, k)
	}
	return namespaces, nil
}

func (m *MetaManager) ListQueues(namespace string) (queues []string, err error) {
	val, err := m.redis.Conn.HGetAll(dummyCtx, join(MetaPrefix, "ns", namespace)).Result()
	if err != nil {
		return nil, err
	}
	for k := range val {
		queues = append(queues, k)
	}
	return queues, nil
}

func (m *MetaManager) initialize() {
	namespaces, err := m.ListNamespaces()
	if err != nil {
		logger.WithField("error", err).Error("initialize meta manager list namespaces error")
		return
	}
	for _, n := range namespaces {
		queues, err := m.ListQueues(n)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"namespace": n,
				"error":     err,
			}).Error("initialize meta manager list queues error")
			return
		}
		for _, q := range queues {
			m.rwmu.Lock()
			m.nsCache[n] = true
			m.qCache[join(n, q)] = true
			m.rwmu.Unlock()
		}
	}
}

func (m *MetaManager) Dump() (map[string][]string, error) {
	data := make(map[string][]string)
	namespaces, err := m.ListNamespaces()
	if err != nil {
		return nil, err
	}
	for _, n := range namespaces {
		queues, err := m.ListQueues(n)
		if err != nil {
			return nil, err
		}
		data[n] = queues
	}
	return data, nil
}
