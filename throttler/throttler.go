package throttler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/helper"
)

const (
	throttlerRedisKey = "__throttler_keys__"

	throttleIncrLuaScript = `
local key = KEYS[1]
local ttl = tonumber(ARGV[1])
local v = redis.call("incr", key)
if v == 1 then 
	redis.call("expire", key, ttl)
end
return v
`
	throttleDecrLuaScript = `
local key = KEYS[1]
local exists = redis.call("exists", key)
if exists == 1 then
	return redis.call('decr', key)
end
`
)

var (
	dummyCtx = context.TODO()
)

// Limit is the detail limit of the token
type Limiter struct {
	Read     int64 `json:"read"`
	Write    int64 `json:"write"`
	Interval int64 `json:"interval"`
}

// TokenLimit is limit of the token
type TokenLimiter struct {
	Namespace string  `json:"namespace"`
	Token     string  `json:"token"`
	Limiter   Limiter `json:"limiter"`
}

// Throttler is the QPS throttler for publish/consume
type Throttler struct {
	redisCli *redis.Client
	mu       sync.RWMutex
	cache    map[string]*Limiter
	stop     chan bool
	incrSHA  string
	decrSHA  string
	logger   *logrus.Logger
}

var _throttler *Throttler

func (t *Throttler) buildLimitKey(pool, namespace, token string) string {
	return fmt.Sprintf("%s/%s/%s", pool, namespace, token)
}

func (t *Throttler) buildCounterKey(pool, namespace, token string, isRead bool) string {
	if isRead {
		return fmt.Sprintf("l/%s/%s", t.buildLimitKey(pool, namespace, token), "r")
	}
	return fmt.Sprintf("l/%s/%s", t.buildLimitKey(pool, namespace, token), "w")
}

// RemedyLimiter would remedy the limiter when consume/produce go wrong
func (t *Throttler) RemedyLimiter(pool, namespace, token string, isRead bool) error {
	limiter := t.Get(pool, namespace, token)
	if limiter == nil {
		return nil
	}
	if (isRead && limiter.Read <= 0) || (!isRead && limiter.Write <= 0) {
		return nil
	}

	tokenCounterKey := t.buildCounterKey(pool, namespace, token, isRead)
	_, err := t.redisCli.EvalSha(dummyCtx, t.decrSHA, []string{tokenCounterKey}).Result()
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		sha, err := t.redisCli.ScriptLoad(dummyCtx, throttleDecrLuaScript).Result()
		if err != nil {
			return err
		}
		t.decrSHA = sha
		_, err = t.redisCli.EvalSha(dummyCtx, t.decrSHA, []string{tokenCounterKey}).Result()
	}
	return err
}

// GetAll return all limiters
func (t *Throttler) GetAll(forceUpdate bool) []TokenLimiter {
	var token string
	limiters := make([]TokenLimiter, 0)
	if forceUpdate {
		t.updateLimiters()
	}
	t.mu.RLock()
	for name, limiter := range t.cache {
		fields := strings.Split(name, "/")
		if len(fields) != 3 {
			continue
		}
		token = fields[2]
		if fields[0] != config.DefaultPoolName {
			token = fields[0] + ":" + fields[2]
		}
		limiters = append(limiters, TokenLimiter{
			Namespace: fields[1],
			Token:     token,
			Limiter:   *limiter,
		})
	}
	t.mu.RUnlock()
	return limiters
}

// IsReachLimit check whether the read or write op was reached limit
func (t *Throttler) IsReachRateLimit(pool, namespace, token string, isRead bool) (bool, error) {
	limiter := t.Get(pool, namespace, token)
	if limiter == nil {
		return false, nil
	}
	if (isRead && limiter.Read <= 0) || (!isRead && limiter.Write <= 0) {
		return false, nil
	}

	tokenCounterKey := t.buildCounterKey(pool, namespace, token, isRead)
	val, err := t.redisCli.EvalSha(dummyCtx, t.incrSHA, []string{tokenCounterKey}, limiter.Interval).Result()
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		sha, err := t.redisCli.ScriptLoad(dummyCtx, throttleIncrLuaScript).Result()
		if err != nil {
			return true, fmt.Errorf("failed to load the throttler incr script err: %s", err.Error())
		}
		t.incrSHA = sha
		val, err = t.redisCli.EvalSha(dummyCtx, t.incrSHA, []string{tokenCounterKey}, limiter.Interval).Result()
	}
	if err != nil {
		return true, fmt.Errorf("failed to eval the throttler incr script err: %s", err.Error())
	}
	if isRead {
		return val.(int64) > limiter.Read, nil
	}
	return val.(int64) > limiter.Write, nil
}

// Add would add the token into the throttler,
// return error if the token has already exists
func (t *Throttler) Add(pool, namespace, token string, limiter *Limiter) error {
	if err := limiter.validate(); err != nil {
		return err
	}
	bytes, err := json.Marshal(limiter)
	if err != nil {
		return err
	}
	tokenLimitKey := t.buildLimitKey(pool, namespace, token)
	ok, err := t.redisCli.HSetNX(dummyCtx, throttlerRedisKey, tokenLimitKey, string(bytes)).Result()
	if err != nil {
		return fmt.Errorf("throttler add token, %s", err.Error())
	} else if !ok {
		return fmt.Errorf("throttler token(%s) has already exists", token)
	}
	t.mu.Lock()
	t.cache[tokenLimitKey] = limiter
	t.mu.Unlock()
	return nil
}

// Get the token's limiter if exists
func (t *Throttler) Get(pool, namespace, token string) *Limiter {
	tokenLimitKey := t.buildLimitKey(pool, namespace, token)
	t.mu.RLock()
	if limiter, ok := t.cache[tokenLimitKey]; ok {
		t.mu.RUnlock()
		return limiter
	}
	t.mu.RUnlock()
	return nil
}

// Set would set the token into the throttler
func (t *Throttler) Set(pool, namespace, token string, limiter *Limiter) error {
	if err := limiter.validate(); err != nil {
		return err
	}
	bytes, err := json.Marshal(limiter)
	if err != nil {
		return err
	}

	tokenLimitKey := t.buildLimitKey(pool, namespace, token)
	_, err = t.redisCli.HSet(dummyCtx, throttlerRedisKey, tokenLimitKey, string(bytes)).Result()
	if err != nil {
		return fmt.Errorf("throttler set token(%s), %s", token, err.Error())
	}
	t.mu.Lock()
	t.cache[tokenLimitKey] = limiter
	t.mu.Unlock()
	return nil
}

// Delete would the token from the throttler
func (t *Throttler) Delete(pool, namespace, token string) error {
	tokenLimitKey := t.buildLimitKey(pool, namespace, token)
	_, err := t.redisCli.HDel(dummyCtx, throttlerRedisKey, tokenLimitKey).Result()
	if err != nil {
		return fmt.Errorf("throttler delete token(%s), %s", token, err.Error())
	}
	t.mu.Lock()
	delete(t.cache, tokenLimitKey)
	t.mu.Unlock()
	return nil
}

// Shutdown would stop the throttler async update goroutine
func (t *Throttler) Shutdown() {
	close(t.stop)
	t.redisCli.Close()
}

func (t *Throttler) updateLimiters() {
	// CAUTION: assume throttler key set was smaller, always fetch at once
	results, err := t.redisCli.HGetAll(dummyCtx, throttlerRedisKey).Result()
	if err != nil {
		t.logger.Errorf("Failed to fetch the throttler tokens, encounter err: %s", err.Error())
		return
	}

	var limiter Limiter
	newCache := make(map[string]*Limiter, 0)
	for token, limiterString := range results {
		if err := json.Unmarshal([]byte(limiterString), &limiter); err != nil {
			t.logger.Warnf("Failed to marshal token(%s) limiter, encounter err: %s", token, err.Error())
			continue
		}
		newCache[token] = &limiter
	}
	// unnecessary to lock the cache here
	t.cache = newCache
}

func (t *Throttler) asyncLoop() {
	t.updateLimiters()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-t.stop:
			t.logger.Info("Throttler would be exited while the stop signal was received")
			return
		case <-ticker.C:
			t.updateLimiters()
		}
	}
}

func (l *Limiter) validate() error {
	if l.Interval <= 0 {
		return errors.New("limiter interval should be >= 0")
	}
	if l.Read == 0 && l.Write == 0 {
		return errors.New("the read and write of limiter can't be 0 at the same time")
	}
	return nil
}

// GetThrottler return the global throttler
func GetThrottler() *Throttler {
	return _throttler
}

// Setup create new throttler
func Setup(conf *config.RedisConf, logger *logrus.Logger) error {
	cli := helper.NewRedisClient(conf, nil)
	sha1, err := cli.ScriptLoad(dummyCtx, throttleIncrLuaScript).Result()
	if err != nil {
		return fmt.Errorf("load the throttle incr script: %s", err.Error())
	}
	sha2, err := cli.ScriptLoad(dummyCtx, throttleDecrLuaScript).Result()
	if err != nil {
		return fmt.Errorf("load the throttle decr script: %s", err.Error())
	}
	_throttler = &Throttler{
		redisCli: cli,
		logger:   logger,
		incrSHA:  sha1,
		decrSHA:  sha2,
		stop:     make(chan bool),
		cache:    make(map[string]*Limiter, 0),
	}
	go _throttler.asyncLoop()
	return nil
}
