package redis_v2

import (
	"fmt"
	"sort"
	"sync"

	go_redis "github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

const (
	MetaSubscribeChannel = "_lmstfy_v2_meta_pubsub_channel_"
	MetaPrefix           = "_lmstfy_v2_meta_"
)

// Record meta and update from redis periodically
type meta struct {
	namespace string
	queue     string
}

type MetaManager struct {
	redis      *RedisInstance
	pubsub     *go_redis.PubSub
	existCache map[string]map[string]bool
	cache      []meta
	rwmu       sync.RWMutex
	stop       chan bool
}

func NewMetaManager(redis *RedisInstance) (*MetaManager, error) {
	m := &MetaManager{
		redis:      redis,
		existCache: make(map[string]map[string]bool),
		cache:      make([]meta, 0),
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

func (m *MetaManager) Exist(namespace, queue string) bool {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	if _, ok := m.existCache[namespace]; !ok {
		return false
	}
	return m.existCache[namespace][queue]
}

func (m *MetaManager) Add(namespace, queue string) error {
	// did not check exist, because add is rarely operation and update maybe late for a while
	_, err := m.redis.Conn.HSet(dummyCtx, join(MetaPrefix, "ns"), namespace, 1).Result()
	if err != nil {
		return fmt.Errorf("meta manager add meta ns error, %v", err)
	}
	_, err = m.redis.Conn.HSet(dummyCtx, join(MetaPrefix, "ns", namespace), queue, 1).Result()
	if err != nil {
		return fmt.Errorf("meta manager add meta queue error, %v", err)
	}
	return m.publish()
}

func (m *MetaManager) Remove(namespace, queue string) error {
	// did not check exist, because remove is rarely operation and update maybe late for a while
	_, err := m.redis.Conn.HDel(dummyCtx, join(MetaPrefix, "ns", namespace), queue).Result()
	if err != nil {
		return fmt.Errorf("meta manager remove meta queue error, %v", err)
	}
	return m.publish()
}

func (m *MetaManager) listNamespaces() (namespaces []string, err error) {
	val, err := m.redis.Conn.HGetAll(dummyCtx, join(MetaPrefix, "ns")).Result()
	if err != nil {
		return nil, err
	}
	for k := range val {
		namespaces = append(namespaces, k)
	}
	return namespaces, nil
}

func (m *MetaManager) listQueues(namespace string) (queues []string, err error) {
	val, err := m.redis.Conn.HGetAll(dummyCtx, join(MetaPrefix, "ns", namespace)).Result()
	if err != nil {
		return nil, err
	}
	for k := range val {
		queues = append(queues, k)
	}
	return queues, nil
}

func (m *MetaManager) update() error {
	namespaces, err := m.listNamespaces()
	if err != nil {
		return fmt.Errorf("meta manager list namsepace error, %v", err)
	}
	existCache := make(map[string]map[string]bool)
	cache := make([]meta, 0)
	for _, n := range namespaces {
		queues, err := m.listQueues(n)
		if err != nil {
			return fmt.Errorf("meta manager list %s queue error, %v", n, err)
		}
		for _, q := range queues {
			m := meta{
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

func (m *MetaManager) subscribe() error {
	m.pubsub = m.redis.Conn.Subscribe(dummyCtx, MetaSubscribeChannel)
	_, err := m.pubsub.Receive(dummyCtx)
	if err != nil {
		return fmt.Errorf("subscribe meta updata channel error, %v", err)
	}
	return nil
}

func (m *MetaManager) publish() error {
	_, err := m.redis.Conn.Publish(dummyCtx, MetaSubscribeChannel, "update").Result()
	if err != nil {
		return fmt.Errorf("meta manager publish update error, %v", err)
	}
	return nil
}

func (m *MetaManager) watch() {
	ch := m.pubsub.Channel()
	for {
		select {
		case <-m.stop:
			if err := m.pubsub.Unsubscribe(dummyCtx, MetaSubscribeChannel); err != nil {
				logger.WithFields(logrus.Fields{
					"error": err,
				}).Error("meta manager unsubscribe error")
			}
			m.pubsub.Close()
			logger.Info("meta watcher would be exited while the stop signal was received")
			return
		case msg := <-ch:
			if err := m.update(); err != nil {
				logger.WithFields(logrus.Fields{
					"error":   err,
					"message": msg,
				}).Error("update meta info error")
			}
		}
	}
}

func (m *MetaManager) Close() {
	close(m.stop)
}

func (m *MetaManager) Dump() (map[string][]string, error) {
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
