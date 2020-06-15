package config

import "testing"

func TestConfig_Validate(t *testing.T) {
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
	conf.mode = sentinelMode
	if err := conf.validate(); err == nil {
		t.Fatalf("no master name error was expected, but got nil")
	}
	conf.MasterName = "test"
	if err := conf.validate(); err != nil {
		t.Fatalf("no error was expected, but got %v", err)
	}
}
