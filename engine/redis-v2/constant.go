package redis_v2

const (
	DelayQueuePrefix = "_v2_t"
	PoolPrefix       = "_v2_j"
	ReadyQueuePrefix = "_v2_q"
	DeadLetterPrefix = "_v2_d"

	BatchSize = int64(100)
)
