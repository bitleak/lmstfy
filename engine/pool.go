package engine

import "github.com/bitleak/lmstfy/config"

const (
	KindRedis     = "redis"
	KindRedisV2   = "redis_v2"
	KindMigration = "migration"
)

var engines = make(map[string]map[string]Engine)

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

func GetPools() []string {
	return GetPoolsByKind(KindRedis)
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
	kinds := []string{KindRedis, KindRedisV2, KindMigration}
	for _, kind := range kinds {
		if e := GetEngineByKind(kind, pool); e != nil {
			return e
		}
	}
	return nil
}

func Register(kind, pool string, e Engine) {
	if _, ok := engines[kind]; !ok {
		engines[kind] = make(map[string]Engine)
	}
	engines[kind][pool] = e
}

func Shutdown() {
	for kind, enginePool := range engines {
		for name, engine := range enginePool {
			engine.Shutdown()
			delete(enginePool, name)
		}
		delete(engines, kind)
	}
}
