package storage

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains storage related metrics
type Metrics struct {
	storageAddJobs *prometheus.CounterVec
	storageDelJobs *prometheus.CounterVec
	storageSizes   *prometheus.GaugeVec
}

var (
	metrics *Metrics
)

const (
	Namespace = "infra"
	Subsystem = "lmstfy_v2_storage"
)

func setupMetrics() {
	cv := newCounterVecHelper
	gv := newGaugeVecHelper
	metrics = &Metrics{
		storageAddJobs: cv("timer_add_jobs"),
		storageDelJobs: cv("timer_del_jobs"),
		storageSizes:   gv("timer_sizes"),
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

func init() {
	setupMetrics()
}
