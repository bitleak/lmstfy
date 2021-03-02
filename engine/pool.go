package engine

import "github.com/bitleak/lmstfy/config"

const (
	KindRedis     = "redis"
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
	e := GetEngineByKind(KindMigration, pool)
	if e != nil {
		return e
	}
	return GetEngineByKind(KindRedis, pool)
}

func Register(kind, pool string, e Engine) {
	if p, ok := engines[kind]; ok {
		p[pool] = e
	} else {
		p = make(map[string]Engine)
		p[pool] = e
		engines[kind] = p
	}
}

func Shutdown() {
	for _, enginePool := range engines {
		for _, engine := range enginePool {
			engine.Shutdown()
		}
	}
}
