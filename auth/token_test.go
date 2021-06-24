package auth

import (
	"fmt"
	"os"
	"testing"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	redis_engine "github.com/bitleak/lmstfy/engine/redis"
	"github.com/bitleak/lmstfy/helper/redis"

	go_redis "github.com/go-redis/redis/v8"
)

var (
	conf       *config.Config
	adminRedis *go_redis.Client
)

func init() {
	cfg := os.Getenv("LMSTFY_TEST_CONFIG")
	if cfg == "" {
		panic(`
############################################################
PLEASE setup env LMSTFY_TEST_CONFIG to the config file first
############################################################
`)
	}
	var err error
	if conf, err = config.MustLoad(os.Getenv("LMSTFY_TEST_CONFIG")); err != nil {
		panic(fmt.Sprintf("Failed to load config file: %s", err))
	}

}

func setup() {
	if err := Setup(conf); err != nil {
		panic(fmt.Sprintf("Failed to setup auth testcase: %s", err))
	}

	adminRedis = redis.NewClient(&conf.AdminRedis, nil)
	err := adminRedis.Ping(dummyCtx).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to ping: %s", err))
	}
	err = adminRedis.FlushDB(dummyCtx).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to flush db: %s", err))
	}
}

func TestMain(m *testing.M) {
	setup()
	ret := m.Run()
	os.Exit(ret)
}

func TestTokenKey(t *testing.T) {
	tk1 := tokenKey("", "test-ns")
	if tk1 != "tk/"+config.DefaultPoolName+"/test-ns" {
		t.Fatalf("Mismatch tokenKey")
	}

	tk2 := tokenKey("test-pool", "test-ns")
	if tk2 != "tk/test-pool/test-ns" {
		t.Fatalf("Mismatch tokenKey")
	}
}

func TestCacheKey(t *testing.T) {
	tk1 := cacheKey("", "test-ns", "test-new-token")
	if tk1 != config.DefaultPoolName+"test-nstest-new-token" {
		t.Fatalf("Mismatch cacheKey")
	}

	tk2 := cacheKey("test-pool", "test-ns", "test-new-token")
	if tk2 != "test-pooltest-nstest-new-token" {
		t.Fatalf("Mismatch cacheKey")
	}
}

func TestTokenManager_New(t *testing.T) {
	_, err := GetTokenManager().New("", "test-ns", "test-new-token", "")
	if err != ErrPoolNotExist {
		t.Fatalf("Expected new token return pool not exist error, but got %v", err)
	}

	// New token in default pool
	engine.Register(engine.KindRedis, config.DefaultPoolName, &redis_engine.Engine{})
	tk, err := GetTokenManager().New("", "test-ns", "test-new-token", "")
	if err != nil {
		t.Fatalf("Expected new token return nil, but got %v", err)
	}
	defer GetTokenManager().Delete("", "test-ns", "test-new-token")
	if tk != "test-new-token" {
		t.Fatalf("Expected new token return test-new-token, but got %v", tk)
	}

	// Check token in redis
	ok, err := adminRedis.HExists(dummyCtx, tokenKey("", "test-ns"), "test-new-token").Result()
	if err != nil {
		t.Fatalf("Expected check token exist return nil, but got %v", err)
	}
	if !ok {
		t.Fatalf("Expected check token exist, but not exist")
	}
	// Check token in cache
	if !GetTokenManager().cache[cacheKey("", "test-ns", "test-new-token")] {
		t.Fatalf("Expected check token cache exist, but not exist")
	}

	engine.Register(engine.KindRedis, config.DefaultPoolName, &redis_engine.Engine{})
	_, err = GetTokenManager().New("", "test-ns", "test-new-token", "")
	if err != ErrTokenExist {
		t.Fatalf("Expected new token return token exsit error, but got %v", err)
	}

	// New token in custom pool
	engine.Register(engine.KindRedis, "test-pool", &redis_engine.Engine{})
	tk, err = GetTokenManager().New("test-pool", "test-ns", "test-new-token", "")
	if err != nil {
		t.Fatalf("Expected new token return nil, but got %v", err)
	}
	defer GetTokenManager().Delete("test-pool", "test-ns", "test-new-token")
	if tk != "test-pool:test-new-token" {
		t.Fatalf("Expected new token return test-pool:test-new-token, but got %v", tk)
	}
}

func TestTokenManager_Exist(t *testing.T) {
	engine.Register(engine.KindRedis, config.DefaultPoolName, &redis_engine.Engine{})
	tk, err := GetTokenManager().New("", "test-ns", "test-exist-token", "")
	if err != nil {
		t.Fatalf("Expected new token return nil, but got %v", err)
	}
	defer GetTokenManager().Delete("", "test-ns", "test-new-token")

	// Check token exist in memory cache
	ok, err := GetTokenManager().Exist("", "test-ns", tk)
	if err != nil {
		t.Fatalf("Expected token exist return nil, but got %v", err)
	}
	if !ok {
		t.Fatalf("Expected token exist")
	}

	delete(GetTokenManager().cache, cacheKey("", "test-ns", tk))

	// Check token exist in redis and write back to cache
	ok, err = GetTokenManager().Exist("", "test-ns", tk)
	if err != nil {
		t.Fatalf("Expected token exist return nil, but got %v", err)
	}
	if !ok {
		t.Fatalf("Expected token exist")
	}
	if !GetTokenManager().cache[cacheKey("", "test-ns", tk)] {
		t.Fatalf("Expected check token cache exist, but not exist")
	}

	// Check pool not exist
	_, err = GetTokenManager().Exist("not-exist", "test-ns", tk)
	if err != ErrPoolNotExist {
		t.Fatalf("Expected exist token return pool not exist error, but got %v", err)
	}
}

func TestTokenManager_Delete(t *testing.T) {
	engine.Register(engine.KindRedis, config.DefaultPoolName, &redis_engine.Engine{})
	tk, err := GetTokenManager().New("", "test-ns", "test-delete-token", "")
	if err != nil {
		t.Fatalf("Expected new token return nil, but got %v", err)
	}
	err = GetTokenManager().Delete("", "test-ns", tk)
	if err != nil {
		t.Fatalf("Expected delete token return nil, but got %v", err)
	}

	// Check token deleted in cache and redis
	if GetTokenManager().cache[cacheKey("", "test-ns", tk)] {
		t.Fatalf("Expected check token cache not exist, but exist")
	}
	ok, err := adminRedis.HExists(dummyCtx, tokenKey("", "test-ns"), tk).Result()
	if err != nil {
		t.Fatalf("Expected check token in redis exist return nil, but got %v", err)
	}
	if ok {
		t.Fatalf("Expected check token in redis not exist, but exist")
	}

	err = GetTokenManager().Delete("not-exist", "test-ns", tk)
	if err != ErrPoolNotExist {
		t.Fatalf("Expected delete token return pool not exist error, but got %v", err)
	}
}

func TestTokenManager_List(t *testing.T) {
	engine.Register(engine.KindRedis, config.DefaultPoolName, &redis_engine.Engine{})
	tk, err := GetTokenManager().New("", "test-ns", "test-list-token", "")
	if err != nil {
		t.Fatalf("Expected new token return nil, but got %v", err)
	}
	defer GetTokenManager().Delete("", "test-ns", "test-list-token")

	// List tokens in default pool
	tokens, err := GetTokenManager().List("", "test-ns")
	if err != nil {
		t.Fatalf("Expected list token return nil, but got %v", err)
	}
	if _, ok := tokens[tk]; !ok {
		t.Fatalf("Expected list token contains test-list-token")
	}

	engine.Register(engine.KindRedis, "test-pool", &redis_engine.Engine{})
	tk, err = GetTokenManager().New("test-pool", "test-ns", "test-list-token", "")
	if err != nil {
		t.Fatalf("Expected new token return nil, but got %v", err)
	}
	defer GetTokenManager().Delete("test-pool", "test-ns", "test-list-token")

	// List tokens in custom pool
	tokens, err = GetTokenManager().List("test-pool", "test-ns")
	if err != nil {
		t.Fatalf("Expected list token return nil, but got %v", err)
	}
	if _, ok := tokens[tk]; !ok {
		t.Fatalf("Expected list token contains test-pool:test-list-token")
	}

	// List tokens in not exist pool
	_, err = GetTokenManager().List("not-exist", "test-ns")
	if err != ErrPoolNotExist {
		t.Fatalf("Expected list token return pool not exist error, but got %v", err)
	}
}
