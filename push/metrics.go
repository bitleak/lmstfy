package push

import "github.com/prometheus/client_golang/prometheus"

type PerformanceMetrics struct {
	ConsumeLatencies *prometheus.HistogramVec
	PushLatencies    *prometheus.HistogramVec
	PushHTTPCodes    *prometheus.CounterVec
}

var metrics *PerformanceMetrics

func setupMetrics() {
	metrics = &PerformanceMetrics{}
	consumeLatencies := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "infra",
			Subsystem: "lmstfy_pusher",
			Name:      "consume_latency_milliseconds",
			Help:      "latencies",
			Buckets:   prometheus.ExponentialBuckets(8, 2.5, 8),
		},
		[]string{"pool", "namespace", "queue"},
	)

	pushLatencies := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "infra",
			Subsystem: "lmstfy_pusher",
			Name:      "push_latency_milliseconds",
			Help:      "push http latencies",
			Buckets:   prometheus.ExponentialBuckets(50, 2.5, 9),
		},
		[]string{"pool", "namespace", "queue"},
	)

	httpCodes := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "infra",
			Subsystem: "lmstfy_pusher",
			Name:      "push_http_codes",
			Help:      "push response code",
		},
		[]string{"pool", "namespace", "queue", "code"},
	)

	prometheus.MustRegister(consumeLatencies)
	prometheus.MustRegister(pushLatencies)
	prometheus.MustRegister(httpCodes)
	metrics.ConsumeLatencies = consumeLatencies
	metrics.PushLatencies = pushLatencies
	metrics.PushHTTPCodes = httpCodes
}
