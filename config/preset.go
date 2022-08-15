package config

import (
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/redis"
)

type PresetConfigForTest struct {
	*Config
	containers []*gnomock.Container
}

func CreatePresetForTest(pools ...string) *PresetConfigForTest {
	cfg := &Config{
		Host:      "127.0.0.1",
		Port:      7777,
		AdminHost: "127.0.0.1",
		AdminPort: 7778,
		LogLevel:  "INFO",
		Pool:      make(map[string]RedisConf),
	}

	p := redis.Preset()
	defaultContainer, _ := gnomock.Start(p)
	addr := defaultContainer.DefaultAddress()
	cfg.AdminRedis.Addr = addr
	cfg.Pool[DefaultPoolName] = RedisConf{Addr: addr}

	containers := []*gnomock.Container{defaultContainer}
	for _, extraPool := range pools {
		if _, ok := cfg.Pool[extraPool]; ok {
			continue
		}
		extraContainer, _ := gnomock.Start(p)
		cfg.Pool[extraPool] = RedisConf{Addr: extraContainer.DefaultAddress()}
		containers = append(containers, extraContainer)
	}
	return &PresetConfigForTest{
		Config:     cfg,
		containers: containers,
	}
}

func (presetConfig *PresetConfigForTest) Destroy() {
	gnomock.Stop(presetConfig.containers...)
	presetConfig.Config = nil
}
