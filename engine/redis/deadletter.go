package redis

import (
	"fmt"
	"time"

	go_redis "github.com/go-redis/redis"
	"github.com/meitu/lmstfy/engine"
)

const (
	LUA_D_RESPAWN = `
local deadletter = KEYS[1]
local queue = KEYS[2]
local poolPrefix = KEYS[3]
local limit = tonumber(ARGV[1])
local respawnTTL = tonumber(ARGV[2])

for i = 1, limit do
    local data = redis.call("RPOPLPUSH", deadletter, queue)
	if data == false then
		return i - 1  -- deadletter is empty
	end
    -- unpack the jobID, and set the TTL
    local _, jobID = struct.unpack("HHc0", data)
    if respawnTTL > 0 then
		redis.call("EXPIRE", poolPrefix .. "/" .. jobID, respawnTTL)
	end
end
return limit  -- deadletter has more data when return value is >= limit
`
	LUA_D_DELETE = `
local deadletter = KEYS[1]
local poolPrefix = KEYS[2]
local limit = tonumber(ARGV[1])

for i = 1, limit do
	local data = redis.call("RPOP", deadletter)
	if data == false then
		return i - 1
	end
	-- unpack the jobID, and delete the job from the job pool
	local _, jobID = struct.unpack("HHc0", data)
	redis.call("DEL", poolPrefix .. "/" .. jobID)
end
return limit
`
)

var (
	_lua_respawn_deadletter_sha string
	_lua_delete_deadletter_sha  string
)

// Because the DeadLetter is not like Timer which is a singleton,
// DeadLetters are transient objects like Queue. So we have to preload
// the lua scripts separately.
func PreloadDeadLetterLuaScript(redis *RedisInstance) {
	sha, err := redis.Conn.ScriptLoad(LUA_D_RESPAWN).Result()
	if err != nil {
		panic(fmt.Sprintf("failed to preload lua script: %s", err))
	}
	_lua_respawn_deadletter_sha = sha

	sha, err = redis.Conn.ScriptLoad(LUA_D_DELETE).Result()
	if err != nil {
		panic(fmt.Sprintf("failed to preload luascript: %s", err))
	}
	_lua_delete_deadletter_sha = sha
}

// DeadLetter is where dead job will be buried, the job can be respawned into ready queue
type DeadLetter struct {
	redis           *RedisInstance
	namespace       string
	queue           string
	lua_respawn_sha string
	lua_delete_sha  string
}

func NewDeadLetter(namespace, queue string, redis *RedisInstance) *DeadLetter {
	dl := &DeadLetter{
		redis:           redis,
		namespace:       namespace,
		queue:           queue,
		lua_respawn_sha: _lua_respawn_deadletter_sha,
		lua_delete_sha:  _lua_delete_deadletter_sha,
	}
	if dl.lua_respawn_sha == "" {
		panic("dead letter's lua script is not preloaded")
	}
	return dl
}

func (dl *DeadLetter) Name() string {
	return join(DeadLetterPrefix, dl.namespace, dl.queue)
}

// Add a job to dead letter. NOTE the data format is the same
// as the ready queue (lua struct `HHc0`), by doing this we could directly pop
// the dead job back to the ready queue.
//
// NOTE: this method is not called any where except in tests, but this logic is
// implement in the timer's LUA_T_PUMP script. please refer to that.
func (dl *DeadLetter) Add(jobID string) error {
	val := structPack(1, jobID)
	if err := dl.redis.Conn.Persist(PoolJobKey2(dl.namespace, dl.queue, jobID)).Err(); err != nil {
		return err
	}
	return dl.redis.Conn.LPush(dl.Name(), val).Err()
}

func (dl *DeadLetter) Peek() (size int64, jobID string, err error) {
	val, err := dl.redis.Conn.LIndex(dl.Name(), -1).Result()
	switch err {
	case nil:
		// continue
	case go_redis.Nil:
		return 0, "", engine.ErrNotFound
	default:
		return 0, "", err
	}
	tries, jobID, err := structUnpack(val)
	if err != nil || tries != 1 {
		return 0, "", fmt.Errorf("failed to unpack data: %s", err)
	}
	size, err = dl.redis.Conn.LLen(dl.Name()).Result()
	if err != nil {
		return 0, "", err
	}
	return size, jobID, nil
}

func (dl *DeadLetter) Delete(limit int64) (count int64, err error) {
	if limit > 1 {
		poolPrefix := PoolJobKeyPrefix(dl.namespace, dl.queue)
		var batchSize int64 = 100
		if batchSize > limit {
			batchSize = limit
		}
		for {
			val, err := dl.redis.Conn.EvalSha(dl.lua_delete_sha, []string{dl.Name(), poolPrefix}, batchSize).Result()
			if err != nil {
				if isLuaScriptGone(err) {
					PreloadDeadLetterLuaScript(dl.redis)
					continue
				}
				return count, err
			}
			n, _ := val.(int64)
			count += n
			if n < batchSize { // Dead letter is empty
				break
			}
			if count >= limit {
				break
			}
			if limit-count < batchSize {
				batchSize = limit - count // This is the last batch, we should't respawn jobs exceeding the limit.
			}
		}
		return count, nil
	} else if limit == 1 {
		data, err := dl.redis.Conn.RPop(dl.Name()).Result()
		if err != nil {
			if err == go_redis.Nil {
				return 0, nil
			}
			return 0, err
		}
		_, jobID, err := structUnpack(data)
		if err != nil {
			return 1, err
		}
		err = dl.redis.Conn.Del(PoolJobKey2(dl.namespace, dl.queue, jobID)).Err()
		if err != nil {
			return 1, fmt.Errorf("failed to delete job data: %s", err)
		}
		return 1, nil
	} else {
		return 0, nil
	}
}

func (dl *DeadLetter) Respawn(limit, ttlSecond int64) (count int64, err error) {
	defer func() {
		if err != nil && count != 0 {
			metrics.deadletterRespawnJobs.WithLabelValues(dl.redis.Name).Add(float64(count))
		}
	}()
	queueName := (&QueueName{
		Namespace: dl.namespace,
		Queue:     dl.queue,
	}).String()
	poolPrefix := PoolJobKeyPrefix(dl.namespace, dl.queue)
	if limit > 1 {
		var batchSize int64 = 100
		if batchSize > limit {
			batchSize = limit
		}
		for {
			val, err := dl.redis.Conn.EvalSha(dl.lua_respawn_sha, []string{dl.Name(), queueName, poolPrefix}, batchSize, ttlSecond).Result() // Respawn `batchSize` jobs at a time
			if err != nil {
				if isLuaScriptGone(err) {
					PreloadDeadLetterLuaScript(dl.redis)
					continue
				}
				return 0, err
			}
			n, _ := val.(int64)
			count += n
			if n < batchSize { // Dead letter is empty
				break
			}
			if count >= limit {
				break
			}
			if limit-count < batchSize {
				batchSize = limit - count // This is the last batch, we should't respawn jobs exceeding the limit.
			}
		}
		return count, nil
	} else if limit == 1 {
		data, err := dl.redis.Conn.RPopLPush(dl.Name(), queueName).Result()
		if err != nil {
			if err == go_redis.Nil {
				return 0, nil
			}
			return 0, err
		}
		_, jobID, err := structUnpack(data)
		if err != nil {
			return 1, err
		}
		if ttlSecond > 0 {
			err = dl.redis.Conn.Expire(PoolJobKey2(dl.namespace, dl.queue, jobID), time.Duration(ttlSecond)*time.Second).Err()
		}
		if err != nil {
			return 1, fmt.Errorf("failed to set TTL on respawned job[%s]: %s", jobID, err)
		}
		return 1, nil
	} else {
		return 0, nil
	}
}

func (dl *DeadLetter) Size() (size int64, err error) {
	return dl.redis.Conn.LLen(dl.Name()).Result()
}
