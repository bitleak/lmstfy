package redis_v2

import (
	"strings"
	"sync"
	"time"

	"github.com/oif/gokit/wait"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	// engine related metrics
	publishJobs           *prometheus.CounterVec
	consumeJobs           *prometheus.CounterVec
	consumeMultiJobs      *prometheus.CounterVec
	poolAddJobs           *prometheus.CounterVec
	poolGetJobs           *prometheus.CounterVec
	poolDeleteJobs        *prometheus.CounterVec
	timerAddJobs          *prometheus.CounterVec
	timerDueJobs          *prometheus.CounterVec
	timerFullBatches      *prometheus.CounterVec
	queueDirectPushJobs   *prometheus.CounterVec
	queuePopJobs          *prometheus.CounterVec
	deadletterRespawnJobs *prometheus.CounterVec
	publishQueueJobs      *prometheus.CounterVec
	consumeQueueJobs      *prometheus.CounterVec
	jobElapsedMS          *prometheus.HistogramVec
	jobAckElapsedMS       *prometheus.HistogramVec

	timerSizes      *prometheus.GaugeVec
	queueSizes      *prometheus.GaugeVec
	deadletterSizes *prometheus.GaugeVec

	// redis instance related metrics
	redisMaxMem    *prometheus.GaugeVec
	redisMemUsed   *prometheus.GaugeVec
	redisConns     *prometheus.GaugeVec
	redisBlockings *prometheus.GaugeVec
	redisKeys      *prometheus.GaugeVec
	redisExpires   *prometheus.GaugeVec
}

var (
	metrics *Metrics
)

const (
	Namespace = "infra"
	Subsystem = "lmstfy_redis"
)

func setupMetrics() {
	cv := newCounterVecHelper
	gv := newGaugeVecHelper
	hv := newHistogramHelper
	metrics = &Metrics{
		publishJobs:           cv("v2_publish_jobs"),
		consumeJobs:           cv("v2_consume_jobs"),
		consumeMultiJobs:      cv("v2_consume_multi_jobs"),
		poolAddJobs:           cv("v2_pool_add_jobs"),
		poolGetJobs:           cv("v2_pool_get_jobs"),
		poolDeleteJobs:        cv("v2_pool_delete_jobs"),
		timerAddJobs:          cv("v2_timer_add_jobs"),
		timerDueJobs:          cv("v2_timer_due_jobs"),
		timerFullBatches:      cv("v2_timer_full_batches"),
		queueDirectPushJobs:   cv("v2_queue_direct_push_jobs"),
		queuePopJobs:          cv("v2_queue_pop_jobs"),
		deadletterRespawnJobs: cv("v2_deadletter_respawn_jobs"),
		publishQueueJobs:      cv("v2_publish_queue_jobs", "namespace", "queue"),
		consumeQueueJobs:      cv("v2_consume_queue_jobs", "namespace", "queue"),
		jobElapsedMS:          hv("v2_job_elapsed_ms", "namespace", "queue"),
		jobAckElapsedMS:       hv("v2_job_ack_elapsed_ms", "namespace", "queue"),

		timerSizes:      gv("v2_timer_sizes", "namespace", "queue"),
		queueSizes:      gv("v2_queue_sizes", "namespace", "queue"),
		deadletterSizes: gv("v2_deadletter_sizes", "namespace", "queue"),

		redisMaxMem:    gv("v2_max_mem_bytes"),
		redisMemUsed:   gv("v2_used_mem_bytes"),
		redisConns:     gv("v2_connections"),
		redisBlockings: gv("v2_blocking_connections"),
		redisKeys:      gv("v2_total_keys"),
		redisExpires:   gv("v2_total_ttl_keys"),
	}
}

func newCounterVecHelper(name string, labels ...string) *prometheus.CounterVec {
	labels = append([]string{"pool"}, labels...) // all metrics has this common field `pool`
	opts := prometheus.CounterOpts{}
	opts.Namespace = Namespace
	opts.Subsystem = Subsystem
	opts.Name = name
	opts.Help = name
	counters := prometheus.NewCounterVec(opts, labels)
	prometheus.MustRegister(counters)
	return counters
}

func newGaugeVecHelper(name string, labels ...string) *prometheus.GaugeVec {
	labels = append([]string{"pool"}, labels...)
	opts := prometheus.GaugeOpts{}
	opts.Namespace = Namespace
	opts.Subsystem = Subsystem
	opts.Name = name
	opts.Help = name
	gauges := prometheus.NewGaugeVec(opts, labels)
	prometheus.MustRegister(gauges)
	return gauges
}

func newSummaryHelper(name string, labels ...string) *prometheus.SummaryVec {
	labels = append([]string{"pool"}, labels...)
	opts := prometheus.SummaryOpts{}
	opts.Namespace = Namespace
	opts.Subsystem = Subsystem
	opts.Name = name
	opts.Help = name
	opts.Objectives = map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.001}
	summary := prometheus.NewSummaryVec(opts, labels)
	prometheus.MustRegister(summary)
	return summary
}

func newHistogramHelper(name string, labels ...string) *prometheus.HistogramVec {
	labels = append([]string{"pool"}, labels...)
	opts := prometheus.HistogramOpts{}
	opts.Namespace = Namespace
	opts.Subsystem = Subsystem
	opts.Name = name
	opts.Help = name
	opts.Buckets = prometheus.ExponentialBuckets(15, 3.5, 7)
	histogram := prometheus.NewHistogramVec(opts, labels)
	prometheus.MustRegister(histogram)
	return histogram
}

type SizeProvider interface {
	Size() (size int64, err error)
}

type SizeMonitor struct {
	redis     *RedisInstance
	timer     *TimerManager
	providers map[string]SizeProvider

	stopCh chan struct{}
	rwmu   sync.RWMutex
}

func NewSizeMonitor(redis *RedisInstance, timer *TimerManager) *SizeMonitor {
	m := &SizeMonitor{
		redis:     redis,
		timer:     timer,
		providers: make(map[string]SizeProvider),
		stopCh:    make(chan struct{}),
	}

	for _, queue := range timer.queueManager.Queues() {
		m.MonitorIfNotExist(queue.namespace, queue.queue)
	}
	go wait.Keep(m.collect, 5*time.Second, true, m.stopCh)
	return m
}

func (m *SizeMonitor) MonitorIfNotExist(ns, q string) {
	queue := &queue{
		namespace: ns,
		queue:     q,
	}
	m.rwmu.RLock()
	if m.providers[queue.ReadyQueueString()] != nil { // queue and deadletter are monitored together, so only test queue
		m.rwmu.RUnlock()
		return
	}
	m.rwmu.RUnlock()
	m.rwmu.Lock()
	m.providers[queue.ReadyQueueString()] = NewQueue(ns, q, m.redis)
	m.providers[queue.DeadletterString()], _ = NewDeadLetter(ns, q, m.redis)
	m.providers[queue.DelayQueueString()] = NewTimerSize(ns, q, m.redis)
	m.rwmu.Unlock()
}

func (m *SizeMonitor) Remove(ns, q string) {
	queue := &queue{
		namespace: ns,
		queue:     q,
	}
	m.rwmu.Lock()
	delete(m.providers, queue.ReadyQueueString())
	delete(m.providers, queue.DeadletterString())
	delete(m.providers, queue.DelayQueueString())
	metrics.queueSizes.DeleteLabelValues(m.redis.Name, ns, q)
	metrics.deadletterSizes.DeleteLabelValues(m.redis.Name, ns, q)
	metrics.timerSizes.DeleteLabelValues(m.redis.Name, ns, q)
	m.rwmu.Unlock()
}

func (m *SizeMonitor) collect() {
	// note: didn't handle queue deleted
	for _, queue := range m.timer.queueManager.Queues() {
		m.MonitorIfNotExist(queue.namespace, queue.queue)
	}
	m.rwmu.RLock()
	for k, p := range m.providers {
		s, err := p.Size()
		if err != nil {
			continue
		}
		splits := strings.SplitN(k, "/", 3)
		switch splits[0] {
		case ReadyQueuePrefix:
			metrics.queueSizes.WithLabelValues(m.redis.Name, splits[1], splits[2]).Set(float64(s))
		case DeadLetterPrefix:
			metrics.deadletterSizes.WithLabelValues(m.redis.Name, splits[1], splits[2]).Set(float64(s))
		case DelayQueuePrefix:
			metrics.timerSizes.WithLabelValues(m.redis.Name, splits[1], splits[2]).Set(float64(s))
		}
	}
	m.rwmu.RUnlock()
}

func (m *SizeMonitor) Close() {
	if m.stopCh != nil {
		close(m.stopCh)
	}
}

func init() {
	setupMetrics()
}
