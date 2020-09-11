package redis

import (
	"fmt"
	"strings"
	"sync"
	"time"

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
	//sv := newSummaryHelper
    hv := newHistogramHelper
	metrics = &Metrics{
		publishJobs:           cv("publish_jobs"),
		consumeJobs:           cv("consume_jobs"),
		consumeMultiJobs:      cv("consume_multi_jobs"),
		poolAddJobs:           cv("pool_add_jobs"),
		poolGetJobs:           cv("pool_get_jobs"),
		poolDeleteJobs:        cv("pool_delete_jobs"),
		timerAddJobs:          cv("timer_add_jobs"),
		timerDueJobs:          cv("timer_due_jobs"),
		timerFullBatches:      cv("timer_full_batches"),
		queueDirectPushJobs:   cv("queue_direct_push_jobs"),
		queuePopJobs:          cv("queue_pop_jobs"),
		deadletterRespawnJobs: cv("deadletter_respawn_jobs"),
		publishQueueJobs:      cv("publish_queue_jobs", "namespace", "queue"),
		consumeQueueJobs:      cv("consume_queue_jobs", "namespace", "queue"),
		jobElapsedMS:          hv("job_elapsed_ms", "namespace", "queue"),
		jobAckElapsedMS:       hv("job_ack_elapsed_ms", "namespace", "queue"),

		timerSizes:      gv("timer_sizes"),
		queueSizes:      gv("queue_sizes", "namespace", "queue"),
		deadletterSizes: gv("deadletter_sizes", "namespace", "queue"),

		redisMaxMem:    gv("max_mem_bytes"),
		redisMemUsed:   gv("used_mem_bytes"),
		redisConns:     gv("connections"),
		redisBlockings: gv("blocking_connections"),
		redisKeys:      gv("total_keys"),
		redisExpires:   gv("total_ttl_keys"),
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
	timer     *Timer
	providers map[string]SizeProvider

	rwmu sync.RWMutex
}

func NewSizeMonitor(redis *RedisInstance, timer *Timer, preloadData map[string][]string) *SizeMonitor {
	m := &SizeMonitor{
		redis:     redis,
		timer:     timer,
		providers: make(map[string]SizeProvider),
	}
	for ns, queues := range preloadData {
		for _, q := range queues {
			m.MonitorIfNotExist(ns, q)
		}
	}
	return m
}

func (m *SizeMonitor) Loop() {
	for {
		time.Sleep(5 * time.Second)
		m.collect()
	}
}

func (m *SizeMonitor) MonitorIfNotExist(namespace, queue string) {
	qname := fmt.Sprintf("q/%s/%s", namespace, queue)
	m.rwmu.RLock()
	if m.providers[qname] != nil { // queue and deadletter are monitored together, so only test queue
		m.rwmu.RUnlock()
		return
	}
	m.rwmu.RUnlock()
	dname := fmt.Sprintf("d/%s/%s", namespace, queue)
	m.rwmu.Lock()
	m.providers[qname] = NewQueue(namespace, queue, m.redis, nil)
	m.providers[dname], _ = NewDeadLetter(namespace, queue, m.redis)
	m.rwmu.Unlock()
}

func (m *SizeMonitor) Remove(namespace, queue string) {
	qname := fmt.Sprintf("q/%s/%s", namespace, queue)
	dname := fmt.Sprintf("d/%s/%s", namespace, queue)
	m.rwmu.Lock()
	delete(m.providers, qname)
	delete(m.providers, dname)
	metrics.queueSizes.DeleteLabelValues(m.redis.Name, namespace, queue)
	metrics.deadletterSizes.DeleteLabelValues(m.redis.Name, namespace, queue)
	m.rwmu.Unlock()
}

func (m *SizeMonitor) collect() {
	s, err := m.timer.Size()
	if err == nil {
		metrics.timerSizes.WithLabelValues(m.redis.Name).Set(float64(s))
	}
	m.rwmu.RLock()
	for k, p := range m.providers {
		s, err := p.Size()
		if err != nil {
			continue
		}
		splits := strings.SplitN(k, "/", 3)
		switch splits[0] {
		case "q":
			metrics.queueSizes.WithLabelValues(m.redis.Name, splits[1], splits[2]).Set(float64(s))
		case "d":
			metrics.deadletterSizes.WithLabelValues(m.redis.Name, splits[1], splits[2]).Set(float64(s))
		}
	}
	m.rwmu.RUnlock()
}

func init() {
	setupMetrics()
}
