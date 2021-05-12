package redis_v2

import (
	"fmt"
	"sort"
	"sync"

	go_redis "github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

const (
	QueueManagerSubscribeChannel = "_lmstfy_v2_queues_pubsub_channel_"
	QueueManagerPrefix           = "_lmstfy_v2_queues_"
)

// Record queue and update from redis periodically
type queue struct {
	namespace string
	queue     string
}

type QueueManager struct {
	redis      *RedisInstance
	pubsub     *go_redis.PubSub
	existCache map[string]map[string]bool
	cache      []queue
	rwmu       sync.RWMutex
	stop       chan bool
}

func NewQueueManager(redis *RedisInstance) (*QueueManager, error) {
	m := &QueueManager{
		redis:      redis,
		existCache: make(map[string]map[string]bool),
		cache:      make([]queue, 0),
		stop:       make(chan bool),
	}
	if err := m.update(); err != nil {
		return nil, err
	}
	if err := m.subscribe(); err != nil {
		return nil, err
	}
	go m.watch()
	return m, nil
}

func (m *QueueManager) Exist(namespace, queue string) bool {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	if _, ok := m.existCache[namespace]; !ok {
		return false
	}
	return m.existCache[namespace][queue]
}

func (m *QueueManager) Add(namespace, queue string) error {
	// did not check exist, because add is rarely operation and update maybe late for a while
	_, err := m.redis.Conn.HSet(dummyCtx, join(QueueManagerPrefix, "ns"), namespace, 1).Result()
	if err != nil {
		return fmt.Errorf("queue manager add queue ns error, %v", err)
	}
	_, err = m.redis.Conn.HSet(dummyCtx, join(QueueManagerPrefix, "ns", namespace), queue, 1).Result()
	if err != nil {
		return fmt.Errorf("queue manager add queue queue error, %v", err)
	}
	return m.notify()
}

func (m *QueueManager) Remove(namespace, queue string) error {
	// did not check exist, because remove is rarely operation and update maybe late for a while
	_, err := m.redis.Conn.HDel(dummyCtx, join(QueueManagerPrefix, "ns", namespace), queue).Result()
	if err != nil {
		return fmt.Errorf("queue manager remove queue queue error, %v", err)
	}
	return m.notify()
}

func (m *QueueManager) listNamespaces() (namespaces []string, err error) {
	val, err := m.redis.Conn.HGetAll(dummyCtx, join(QueueManagerPrefix, "ns")).Result()
	if err != nil {
		return nil, err
	}
	for k := range val {
		namespaces = append(namespaces, k)
	}
	return namespaces, nil
}

func (m *QueueManager) listQueues(namespace string) (queues []string, err error) {
	val, err := m.redis.Conn.HGetAll(dummyCtx, join(QueueManagerPrefix, "ns", namespace)).Result()
	if err != nil {
		return nil, err
	}
	for k := range val {
		queues = append(queues, k)
	}
	return queues, nil
}

func (m *QueueManager) update() error {
	namespaces, err := m.listNamespaces()
	if err != nil {
		return fmt.Errorf("queue manager list namsepace error, %v", err)
	}
	existCache := make(map[string]map[string]bool)
	cache := make([]queue, 0)
	for _, n := range namespaces {
		queues, err := m.listQueues(n)
		if err != nil {
			return fmt.Errorf("queue manager list %s queue error, %v", n, err)
		}
		for _, q := range queues {
			m := queue{
				namespace: n,
				queue:     q,
			}
			if _, ok := existCache[n]; !ok {
				existCache[n] = make(map[string]bool)
			}
			existCache[n][q] = true
			cache = append(cache, m)
		}
	}
	sort.Slice(cache, func(i, j int) bool {
		if cache[i].namespace < cache[j].namespace {
			return true
		} else if cache[i].namespace > cache[j].namespace {
			return false
		} else {
			return cache[i].queue < cache[j].queue
		}
	})
	m.rwmu.Lock()
	m.existCache = existCache
	m.cache = cache
	m.rwmu.Unlock()
	return nil
}

func (m *QueueManager) subscribe() error {
	m.pubsub = m.redis.Conn.Subscribe(dummyCtx, QueueManagerSubscribeChannel)
	_, err := m.pubsub.Receive(dummyCtx)
	if err != nil {
		return fmt.Errorf("subscribe queue updata channel error, %v", err)
	}
	return nil
}

func (m *QueueManager) notify() error {
	_, err := m.redis.Conn.Publish(dummyCtx, QueueManagerSubscribeChannel, "update").Result()
	if err != nil {
		return fmt.Errorf("queue manager notify update error, %v", err)
	}
	return nil
}

func (m *QueueManager) watch() {
	ch := m.pubsub.Channel()
	for {
		select {
		case <-m.stop:
			if err := m.pubsub.Unsubscribe(dummyCtx, QueueManagerSubscribeChannel); err != nil {
				logger.WithFields(logrus.Fields{
					"error": err,
				}).Error("queue manager unsubscribe error")
			}
			m.pubsub.Close()
			logger.Info("queue watcher would be exited while the stop signal was received")
			return
		case msg := <-ch:
			if err := m.update(); err != nil {
				logger.WithFields(logrus.Fields{
					"error":   err,
					"message": msg,
				}).Error("update queue info error")
			}
		}
	}
}

func (m *QueueManager) Queues() []queue {
	m.rwmu.RLock()
	result := make([]queue, len(m.cache))
	copy(result, m.cache)
	m.rwmu.RUnlock()
	return result
}

func (m *QueueManager) Close() {
	close(m.stop)
}

func (m *QueueManager) Dump() (map[string][]string, error) {
	data := make(map[string][]string)
	m.rwmu.RLock()
	for n := range m.existCache {
		for q := range m.existCache[n] {
			data[n] = append(data[n], q)
		}
	}
	m.rwmu.RUnlock()
	return data, nil
}
