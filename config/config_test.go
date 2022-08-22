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
	conf.MasterName = "test"
	if err := conf.validate(); err != nil {
		t.Fatalf("no error was expected, but got %v", err)
	}
}

func TestConfig_VerifySecStorageConf(t *testing.T) {
	cfg := SpannerConfig{}
	if err := verifySecStorageConf(cfg); err == nil {
		t.Fatal("invalid secondary storage config error was expected, but got nil")
	}
	cfg = SpannerConfig{
		Project:   "test-project",
		Instance:  "test-instance",
		Database:  "test-db",
		TableName: "test-table",
	}
	if err := verifySecStorageConf(cfg); err != nil {
		t.Fatalf("valid secondary storage config expected, but got err %v", err)
	}
}
