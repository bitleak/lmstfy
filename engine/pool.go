package engine

import (
	"fmt"

	"github.com/bitleak/lmstfy/config"
)

const (
	KindRedis     = "redis"
	KindRedisV2   = "redis_v2"
	KindMigration = "migration"
)

var engines = make(map[string]map[string]Engine)

func ValidateKind(kind string) error {
	switch kind {
	case KindRedis, KindRedisV2, KindMigration:
		return nil
	default:
		return fmt.Errorf("invalid engine kind: %s", kind)
	}
}

func GetEngineByKind(kind, pool string) Engine {
	if pool == "" {
		pool = config.DefaultPoolName
	}
	k := engines[kind]
	if k == nil {
		return nil
	}
	return k[pool]
}

func GetPoolsByKind(kind string) []string {
	v, ok := engines[kind]
	if !ok {
		return []string{}
	}
	pools := make([]string, 0)
	for pool := range v {
		pools = append(pools, pool)
	}
	return pools
}

func ExistsPool(pool string) bool {
	if pool == "" {
		pool = config.DefaultPoolName
	}
	return GetEngine(pool) != nil
}

func GetEngine(pool string) Engine {
	if pool == "" {
		pool = config.DefaultPoolName
	}
	e := GetEngineByKind(KindMigration, pool)
	if e != nil {
		return e
	}
	e = GetEngineByKind(KindRedisV2, pool)
	if e != nil {
		return e
	}
	return GetEngineByKind(KindRedis, pool)
}

func Register(kind, pool string, e Engine) {
	if _, ok := engines[kind]; !ok {
		engines[kind] = make(map[string]Engine)
	}
	engines[kind][pool] = e
}

func Shutdown() {
	for _, enginePool := range engines {
		for _, engine := range enginePool {
			engine.Shutdown()
		}
	}
}
