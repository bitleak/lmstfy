package config

import (
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/redis"
)

var SpannerEmulator = &SpannerConfig{
	Project:   "test-project",
	Instance:  "test-instance",
	Database:  "test-db",
	TableName: "lmstfy_jobs",
}

type PresetConfigForTest struct {
	*Config
	containers []*gnomock.Container
}

func CreatePresetForTest(version string, pools ...string) (*PresetConfigForTest, error) {
	cfg := &Config{
		Host:      "127.0.0.1",
		Port:      7777,
		AdminHost: "127.0.0.1",
		AdminPort: 7778,
		LogLevel:  "INFO",
		Pool:      make(map[string]RedisConf),
		SecondaryStorage: &SecondStorage{
			Spanner: SpannerEmulator,
		},
	}

	p := redis.Preset()
	defaultContainer, err := gnomock.Start(p)
	if err != nil {
		return nil, err
	}
	addr := defaultContainer.DefaultAddress()
	cfg.AdminRedis.Addr = addr
	cfg.Pool[DefaultPoolName] = RedisConf{Addr: addr, Version: version}

	containers := []*gnomock.Container{defaultContainer}
	for _, extraPool := range pools {
		if _, ok := cfg.Pool[extraPool]; ok {
			continue
		}
		extraContainer, _ := gnomock.Start(p)
		cfg.Pool[extraPool] = RedisConf{
			Addr:    extraContainer.DefaultAddress(),
			Version: version,
		}
		containers = append(containers, extraContainer)
	}
	return &PresetConfigForTest{
		Config:     cfg,
		containers: containers,
	}, nil
}

func (presetConfig *PresetConfigForTest) Destroy() {
	gnomock.Stop(presetConfig.containers...)
	presetConfig.Config = nil
}
