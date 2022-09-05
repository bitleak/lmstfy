package helper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateRedisConfig(t *testing.T) {
	ctx := context.Background()
	defaultPool := CONF.Pool["default"]
	redisCli := NewRedisClient(&defaultPool, nil)
	_, err := redisCli.ConfigSet(ctx, "appendonly", "no").Result()
	require.Nil(t, err)
	assert.NotNil(t, ValidateRedisConfig(ctx, &defaultPool))
	_, err = redisCli.ConfigSet(ctx, "appendonly", "yes").Result()
	require.Nil(t, err)
	_, err = redisCli.ConfigSet(ctx, "maxmemory-policy", "allkeys-lru").Result()
	require.Nil(t, err)
	assert.NotNil(t, ValidateRedisConfig(ctx, &defaultPool))
	_, err = redisCli.ConfigSet(ctx, "maxmemory-policy", "noeviction").Result()
	defaultPool.EnableSecondaryStorage = true
	assert.NotNil(t, ValidateRedisConfig(ctx, &defaultPool))
	_, err = redisCli.ConfigSet(ctx, "maxmemory", "10000000").Result()
	require.Nil(t, err)

	for _, poolConf := range CONF.Pool {
		assert.Nil(t, ValidateRedisConfig(ctx, &poolConf))
	}
}
