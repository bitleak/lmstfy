package storage

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains storage related metrics
type Metrics struct {
	storageAddJobs      *prometheus.CounterVec
	storageDelJobs      *prometheus.CounterVec
	pumperGetJobLatency *prometheus.HistogramVec
	pumperPubJobLatency *prometheus.HistogramVec
	pumperDelJobLatency *prometheus.HistogramVec
}

var (
	metrics *Metrics
)

const (
	Namespace = "infra"
	Subsystem = "lmstfy_v2_storage"
)

func setupMetrics() {
	cv, hv := newCounterVecHelper, newHistogramHelper

	metrics = &Metrics{
		storageAddJobs:      cv("storage_add_jobs", "namespace", "queue", "status"),
		storageDelJobs:      cv("storage_del_jobs"),
		pumperGetJobLatency: hv("pumper_get_job_latency", "batch_size"),
		pumperPubJobLatency: hv("pumper_pub_job_latency", "batch_size"),
		pumperDelJobLatency: hv("pumper_del_job_latency", "batch_size"),
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

func newHistogramHelper(name string, labels ...string) *prometheus.HistogramVec {
	opts := prometheus.HistogramOpts{}
	opts.Namespace = Namespace
	opts.Subsystem = Subsystem
	opts.Name = name
	opts.Help = name
	opts.Buckets = prometheus.LinearBuckets(5, 20, 25)
	histogram := prometheus.NewHistogramVec(opts, labels)
	prometheus.MustRegister(histogram)
	return histogram
}

func init() {
	setupMetrics()
}
