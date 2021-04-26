package redis_v2

import go_redis "github.com/go-redis/redis/v8"

type RedisInstance struct {
	Name string
	Conn *go_redis.Client
}
