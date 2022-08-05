package redis

const (
	PoolPrefix       = "j"
	QueuePrefix      = "q"
	DeadLetterPrefix = "d"
	MetaPrefix       = "m"
	TimerSetPrefix   = "t"
	StreamIDPrefix   = "s"

	BatchSize = int64(100)
)
