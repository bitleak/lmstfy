package hooks

import (
	"github.com/prometheus/client_golang/prometheus"
)

type performanceMetrics struct {
	Latencies *prometheus.HistogramVec
	QPS       *prometheus.CounterVec
}

var _metrics *performanceMetrics

const (
	_namespace = "infra"
	_subsystem = "lmstfy_redis"
)

func setupMetrics() {
	labels := []string{"node", "command", "status"}
	buckets := prometheus.ExponentialBuckets(1, 2, 16)
	newHistogram := func(name string, labels ...string) *prometheus.HistogramVec {
		histogram := prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: _namespace,
				Subsystem: _subsystem,
				Name:      name,
				Buckets:   buckets,
			},
			labels,
		)
		prometheus.MustRegister(histogram)
		return histogram
	}
	newCounter := func(name string, labels ...string) *prometheus.CounterVec {
		counters := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: _namespace,
				Subsystem: _subsystem,
				Name:      name,
			},
			labels,
		)
		prometheus.MustRegister(counters)
		return counters
	}
	_metrics = &performanceMetrics{
		Latencies: newHistogram("latency", labels...),
		QPS:       newCounter("qps", labels...),
	}
}

func init() {
	setupMetrics()
}
