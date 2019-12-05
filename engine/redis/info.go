package redis

import (
	"strconv"
	"strings"
	"time"
)

type RedisInfo struct {
	MemUsed   int64 // used_memory
	MemMax    int64 // maxmemory
	NKeys     int64 // total keys
	NExpires  int64 // keys with TTL
	NClients  int64 // connected_clients
	NBlocking int64 // blocked_clients
}

func GetRedisInfo(redis *RedisInstance) *RedisInfo {
	info := &RedisInfo{}

	memoryInfo, err := redis.Conn.Info("memory").Result()
	if err == nil {
		lines := strings.Split(memoryInfo, "\r\n")
		for _, l := range lines {
			k, v, _ := parseColonSeparatedKV(l)
			switch k {
			case "used_memory":
				info.MemUsed = v
			case "maxmemory":
				info.MemMax = v
			}
		}
	}
	keyInfo, err := redis.Conn.Info("keyspace").Result()
	if err == nil {
		lines := strings.Split(keyInfo, "\r\n")
		for _, l := range lines {
			splits := strings.SplitN(l, ":", 2)
			if len(splits) != 2 || splits[0] != "db0" {
				continue
			}
			splits2 := strings.SplitN(splits[1], ",", 3)
			for _, s := range splits2 {
				k, v, _ := parseEqualSeparatedKV(s)
				switch k {
				case "keys":
					info.NKeys = v
				case "expires":
					info.NExpires = v
				}
			}
		}
	}
	clientInfo, err := redis.Conn.Info("clients").Result()
	if err == nil {
		lines := strings.Split(clientInfo, "\r\n")
		for _, l := range lines {
			k, v, _ := parseColonSeparatedKV(l)
			switch k {
			case "connected_clients":
				info.NClients = v
			case "blocked_clients":
				info.NBlocking = v
			}
		}
	}

	return info
}

func parseColonSeparatedKV(str string) (key string, value int64, err error) {
	splits := strings.SplitN(str, ":", 2)
	if len(splits) == 2 {
		key = splits[0]
		value, err = strconv.ParseInt(splits[1], 10, 64)
	}
	return
}

func parseEqualSeparatedKV(str string) (key string, value int64, err error) {
	splits := strings.SplitN(str, "=", 2)
	if len(splits) == 2 {
		key = splits[0]
		value, err = strconv.ParseInt(splits[1], 10, 64)
	}
	return
}

func RedisInstanceMonitor(redis *RedisInstance) {
	for {
		time.Sleep(5 * time.Second)
		info := GetRedisInfo(redis)

		metrics.redisMaxMem.WithLabelValues(redis.Name).Set(float64(info.MemMax))
		metrics.redisMemUsed.WithLabelValues(redis.Name).Set(float64(info.MemUsed))
		metrics.redisConns.WithLabelValues(redis.Name).Set(float64(info.NClients))
		metrics.redisBlockings.WithLabelValues(redis.Name).Set(float64(info.NBlocking))
		metrics.redisKeys.WithLabelValues(redis.Name).Set(float64(info.NKeys))
		metrics.redisExpires.WithLabelValues(redis.Name).Set(float64(info.NExpires))
	}
}
