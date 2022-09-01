package storage

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains storage related metrics
type Metrics struct {
	storageAddJobs *prometheus.CounterVec
	storageDelJobs *prometheus.CounterVec
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
	metrics = &Metrics{
		storageAddJobs: cv("storage_add_jobs"),
		storageDelJobs: cv("storage_del_jobs"),
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

func init() {
	setupMetrics()
}
