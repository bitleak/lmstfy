package auth

import (
	"errors"
	"strings"
	"sync"

	"github.com/go-redis/redis"
	"github.com/meitu/lmstfy/config"
	"github.com/meitu/lmstfy/engine"
	"github.com/meitu/lmstfy/uuid"
)

const TokenPrefix = "tk"

var ErrPoolNotExist error = errors.New("the pool was not exists")

type TokenManager struct {
	cli   *redis.Client
	cache map[string]bool // Caching {pool+namespace+token} => bool
	rwmu  sync.RWMutex
}

func tokenKey(pool, namespace string) string {
	if pool == "" {
		pool = config.DefaultPoolName
	}

	var b strings.Builder
	b.Grow(len(TokenPrefix) + len(pool) + len(namespace) + 2)
	b.WriteString(TokenPrefix)
	b.WriteString("/")
	b.WriteString(pool)
	b.WriteString("/")
	b.WriteString(namespace)
	return b.String()
}

func cacheKey(pool, namespace, token string) string {
	if pool == "" {
		pool = config.DefaultPoolName
	}

	var b strings.Builder
	b.Grow(len(pool) + len(namespace) + len(token))
	b.WriteString(pool)
	b.WriteString(namespace)
	b.WriteString(token)
	return b.String()
}

func NewTokenManager(cli *redis.Client) *TokenManager {
	return &TokenManager{
		cli:   cli,
		cache: make(map[string]bool),
	}
}

func (tm *TokenManager) New(pool, namespace, description string) (token string, err error) {
	if exists := engine.ExistsPool(pool); !exists {
		return "", ErrPoolNotExist
	}
	token = uuid.GenUniqueID()
	err = tm.cli.HSetNX(tokenKey(pool, namespace), token, description).Err()
	if err != nil {
		return "", err
	}
	tm.rwmu.Lock()
	tm.cache[cacheKey(pool, namespace, token)] = true
	tm.rwmu.Unlock()
	if pool == "" {
		return token, nil
	}
	return pool + ":" + token, nil
}

func (tm *TokenManager) Exist(pool, namespace, token string) (exist bool, err error) {
	if exists := engine.ExistsPool(pool); !exists {
		return false, ErrPoolNotExist
	}
	tm.rwmu.RLock()
	if tm.cache[cacheKey(pool, namespace, token)] {
		tm.rwmu.RUnlock()
		return true, nil
	}
	tm.rwmu.RUnlock()
	exist, err = tm.cli.HExists(tokenKey(pool, namespace), token).Result()
	if err == nil && exist {
		tm.rwmu.Lock()
		tm.cache[cacheKey(pool, namespace, token)] = true
		tm.rwmu.Unlock()
	}
	return exist, err
}

func (tm *TokenManager) Delete(pool, namespace, token string) error {
	if exists := engine.ExistsPool(pool); !exists {
		return ErrPoolNotExist
	}
	tm.rwmu.Lock()
	delete(tm.cache, cacheKey(pool, namespace, token))
	tm.rwmu.Unlock()
	return tm.cli.HDel(tokenKey(pool, namespace), token).Err()
}

func (tm *TokenManager) List(pool, namespace string) (tokens map[string]string, err error) {
	if exists := engine.ExistsPool(pool); !exists {
		return nil, ErrPoolNotExist
	}
	val, err := tm.cli.HGetAll(tokenKey(pool, namespace)).Result()
	if err != nil {
		return nil, err
	}
	if pool == "" { // Default pool
		return val, nil
	}
	tokens = make(map[string]string)
	for k, v := range val {
		tokens[pool+":"+k] = v
	}
	return tokens, nil
}

var _tokenManager *TokenManager

func Setup(conf *config.Config) {
	cli := redis.NewClient(&redis.Options{
		Addr:     conf.AdminRedis.Addr,
		Password: conf.AdminRedis.Password,
	})
	if cli.Ping().Err() != nil {
		panic("Can not connect to admin redis")
	}
	_tokenManager = NewTokenManager(cli)
}

func GetTokenManager() *TokenManager {
	return _tokenManager
}
