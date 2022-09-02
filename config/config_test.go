package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSecondaryStorageConfig_Validate(t *testing.T) {
	secondaryStorageConfig := SecondaryStorage{}
	assert.Nil(t, secondaryStorageConfig.validate())
	secondaryStorageConfig.Spanner = &SpannerConfig{}
	assert.NotNil(t, secondaryStorageConfig.validate())
	secondaryStorageConfig.Spanner = SpannerEmulator
	assert.Nil(t, secondaryStorageConfig.validate())
}

func TestRedisConfig_Validate(t *testing.T) {
	conf := &RedisConf{}
	if err := conf.validate(); err == nil {
		t.Fatal("validate addr error was expected, but got nil")
	}
	conf.Addr = "abc"
	if err := conf.validate(); err != nil {
		t.Fatalf("no error was expected, but got %v", err)
	}
	conf.DB = -1
	if err := conf.validate(); err == nil {
		t.Fatalf("validate db error was expected, but got nil")
	}
	conf.DB = 0
	conf.MasterName = "test"
	if err := conf.validate(); err != nil {
		t.Fatalf("no error was expected, but got %v", err)
	}

	conf.EnableSecondaryStorage = true
	conf.SecondaryStorageThresholdSeconds = 10
	if err := conf.validate(); err == nil {
		t.Fatalf("validate addr error was expected, but got nil")
	}
}
