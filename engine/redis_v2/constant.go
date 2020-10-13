package redis_v2

const (
	PoolPrefix       = "j"
	QueuePrefix      = "q"
	DeadLetterPrefix = "d"
	MetaPrefix       = "m"
)

const (
	batchSize     = 100
	priorityShift = 41
)
