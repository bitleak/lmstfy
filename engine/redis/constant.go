package redis

const (
	PoolPrefix       = "j"
	QueuePrefix      = "q"
	DeadLetterPrefix = "d"
	MetaPrefix       = "m"

	BatchSize = int64(100)
)
