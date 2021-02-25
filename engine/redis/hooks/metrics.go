package hooks

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	_contextStartTimeKey = iota + 1
	_contextSegmentKey
)

type MetricsHook struct {
	client *redis.Client
}

func NewMetricsHook(client *redis.Client) *MetricsHook {
	return &MetricsHook{client: client}
}

func (hook MetricsHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, _contextStartTimeKey, time.Now()), nil
}

func (hook MetricsHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	hook.record(ctx, cmd.Name(), cmd.Err())
	return nil
}

func (hook MetricsHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, _contextStartTimeKey, time.Now()), nil
}

func (hook MetricsHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	var firstErr error
	for _, cmd := range cmds {
		if cmd.Err() != nil {
			firstErr = cmd.Err()
			break
		}
	}
	hook.record(ctx, "pipeline", firstErr)
	return nil
}

func (hook MetricsHook) record(ctx context.Context, cmd string, err error) {
	startTime, ok := ctx.Value(_contextStartTimeKey).(time.Time)
	if !ok {
		return
	}
	durationMS := time.Since(startTime).Milliseconds()
	status := "ok"
	if err != nil && err != redis.Nil {
		status = "error"
	}
	labels := prometheus.Labels{"node": hook.client.Options().Addr, "command": cmd, "status": status}
	_metrics.QPS.With(labels).Inc()
	_metrics.Latencies.With(labels).Observe(float64(durationMS))
}
