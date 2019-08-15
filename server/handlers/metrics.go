package handlers

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

type PerformanceMetrics struct {
	Latencies *prometheus.SummaryVec
	HTTPCodes *prometheus.CounterVec
}

var metrics *PerformanceMetrics

func setup_metrics() {
	metrics = &PerformanceMetrics{}
	latencies := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:  "infra",
			Subsystem:  "lmstfy_http",
			Name:       "latency_milliseconds",
			Help:       "rest api latencies",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.001},
		},
		[]string{"pool", "namespace", "api"},
	)

	httpCodes := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "infra",
			Subsystem: "lmstfy_http",
			Name:      "http_codes",
			Help:      "rest api response code",
		},
		[]string{"pool", "namespace", "api", "code"},
	)
	prometheus.MustRegister(latencies)
	prometheus.MustRegister(httpCodes)
	metrics.Latencies = latencies
	metrics.HTTPCodes = httpCodes
}

func CollectMetrics(apiName string) func(*gin.Context) {
	return func(c *gin.Context) {
		before := time.Now()
		c.Next()
		after := time.Now()
		duration := after.Sub(before)
		code := c.Writer.Status()
		if code < 300 {
			metrics.Latencies.WithLabelValues(
				c.GetString("pool"),
				c.Param("namespace"),
				apiName).Observe(duration.Seconds() * 1000)
		}
		metrics.HTTPCodes.WithLabelValues(
			c.GetString("pool"),
			c.Param("namespace"),
			apiName,
			strconv.Itoa(code),
		).Inc()
	}
}
