package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/go-cmp/cmp"
	"github.com/sirupsen/logrus"
)

const (
	metaKeyPrefix        = "pm"
	redisMetasKey        = "__push_metas__"
	redisMetasVersionKey = "__push_metas_version__"
)

const (
	maxWorkerNum = 64
	maxTopicNum  = 16
)

var ErrInvalidKey = errors.New("invalid push meta key")
var ErrMetaKeyExists = errors.New("the meta key has already exists")
var ErrMetaKeyNotFound = errors.New("the meta key was not found")

var (
	ctx = context.TODO()
)

type Meta struct {
	Queues   []string `json:"queues"`
	Endpoint string   `json:"endpoint"`
	Workers  int      `json:"workers"`
	Timeout  uint32   `json:"timeout"`
}

type onCreatedFunc func(pool, ns, group string, meta *Meta)
type onUpdatedFunc func(pool, ns, group string, newMeta *Meta)
type onDeletedFunc func(pool, ns, group string)

type MetaManager struct {
	redisCli           *redis.Client
	metas              map[string]*Meta
	logger             *logrus.Logger
	latestMetasVersion int64
	updateInterval     time.Duration

	// callback functions
	onCreated onCreatedFunc
	onUpdated onUpdatedFunc
	onDeleted onDeletedFunc

	stopCh chan struct{}
}

func newMetaManager(
	redisCli *redis.Client,
	updateInterval time.Duration,
	logger *logrus.Logger,
	onCreated onCreatedFunc,
	onUpdated onUpdatedFunc,
	onDeleted onDeletedFunc) (*MetaManager, error) {
	latestMetasVersion, err := redisCli.Get(ctx, redisMetasVersionKey).Int64()
	if err == redis.Nil {
		latestMetasVersion, err = redisCli.Incr(ctx, redisMetasVersionKey).Result()
	}
	mm := &MetaManager{
		redisCli:           redisCli,
		updateInterval:     updateInterval,
		logger:             logger,
		onCreated:          onCreated,
		onUpdated:          onUpdated,
		onDeleted:          onDeleted,
		metas:              make(map[string]*Meta),
		latestMetasVersion: latestMetasVersion,
		stopCh:             make(chan struct{}),
	}
	go mm.asyncLoop()
	return mm, nil
}

func (mm *MetaManager) Close() {
	close(mm.stopCh)
}

func (mm *MetaManager) buildKey(pool, ns, group string) string {
	return fmt.Sprintf("%s/%s/%s/%s", metaKeyPrefix, pool, ns, group)
}

func (mm *MetaManager) splitKey(key string) (string, string, string, error) {
	fields := strings.Split(key, "/")
	if len(fields) != 4 || fields[0] != metaKeyPrefix {
		return "", "", "", ErrInvalidKey
	}
	return fields[1], fields[2], fields[3], nil
}

func (mm *MetaManager) updateMetas() error {
	vals, err := mm.redisCli.HGetAll(ctx, redisMetasKey).Result()
	if err != nil {
		return err
	}
	newMetas := make(map[string]*Meta, len(vals))
	for key, meta := range vals {
		newMeta := new(Meta)
		pool, ns, group, err := mm.splitKey(key)
		if err != nil {
			mm.logger.WithFields(logrus.Fields{
				"key": key,
				"err": err,
			}).Debug("Invalid pusher's key")
			continue
		}
		if err := json.Unmarshal([]byte(meta), newMeta); err != nil {
			mm.logger.WithFields(logrus.Fields{
				"pool":  pool,
				"ns":    ns,
				"group": group,
				"err":   err,
			}).Warn("Failed to marshal the pusher's meta")
			continue
		}
		if oldMeta, ok := mm.metas[key]; ok {
			if !cmp.Equal(newMeta, oldMeta) { // the meta was modified
				mm.onUpdated(pool, ns, group, newMeta)
			}
		} else { // new meta was created
			mm.onCreated(pool, ns, group, newMeta)
		}
		newMetas[key] = newMeta
	}
	for oldKey := range mm.metas {
		pool, ns, group, err := mm.splitKey(oldKey)
		if err != nil {
			mm.logger.WithFields(logrus.Fields{
				"key": oldKey,
				"err": err,
			}).Debug("Invalid pusher's key")
			continue
		}
		if _, ok := newMetas[oldKey]; !ok {
			// the meta was deleted
			mm.onDeleted(pool, ns, group)
		}
	}
	mm.metas = newMetas
	return nil
}

func (mm *MetaManager) asyncLoop() {
	defer func() {
		if err := recover(); err != nil {
			mm.logger.WithFields(logrus.Fields{
				"error": err,
			}).Error("Panic in meta manager")
		}
	}()
	ticker := time.NewTicker(mm.updateInterval)
	mm.updateMetas()
	for {
		select {
		case <-ticker.C:
			latestMetasVersion, err := mm.redisCli.Get(ctx, redisMetasVersionKey).Int64()
			if err != nil {
				if err != redis.Nil {
					mm.logger.WithFields(logrus.Fields{
						"err": err,
					}).Warn("Failed to fetch the metas version key")
				}
				continue
			}
			if latestMetasVersion != mm.latestMetasVersion {
				mm.logger.WithFields(logrus.Fields{
					"local_version":  mm.latestMetasVersion,
					"remote_version": latestMetasVersion,
				}).Info("Update metas while the version was changed")
				mm.updateMetas()
				mm.latestMetasVersion = latestMetasVersion
			}
		case <-mm.stopCh:
			mm.logger.Info("Meta manager would be exited while the stop signal was received")
			return
		}
	}
}

func (mm *MetaManager) GetFromRemote(pool, ns, group string) (*Meta, error) {
	key := mm.buildKey(pool, ns, group)
	metaStr, err := mm.redisCli.HGet(ctx, redisMetasKey, key).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, nil
	}
	meta := new(Meta)
	if err := json.Unmarshal([]byte(metaStr), meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (mm *MetaManager) Get(pool, ns, group string) *Meta {
	key := mm.buildKey(pool, ns, group)
	if pushMeta, ok := mm.metas[key]; ok {
		return pushMeta
	}
	return nil
}

func (mm *MetaManager) Create(pool, ns, group string, meta *Meta) error {
	key := mm.buildKey(pool, ns, group)
	bytes, _ := json.Marshal(meta)
	ok, err := mm.redisCli.HSetNX(ctx, redisMetasKey, key, string(bytes)).Result()
	if err != nil {
		return err
	}
	if !ok {
		return ErrMetaKeyExists
	}
	return mm.redisCli.Incr(ctx, redisMetasVersionKey).Err()
}

func (mm *MetaManager) Update(pool, ns, group string, meta *Meta) error {
	key := mm.buildKey(pool, ns, group)
	bytes, _ := json.Marshal(meta)
	_, err := mm.redisCli.HSet(ctx, redisMetasKey, key, string(bytes)).Result()
	if err != nil {
		return err
	}
	return mm.redisCli.Incr(ctx, redisMetasVersionKey).Err()
}

func (mm *MetaManager) Delete(pool, ns, group string) error {
	key := mm.buildKey(pool, ns, group)
	cnt, err := mm.redisCli.HDel(ctx, redisMetasKey, key).Result()
	if err != nil {
		return err
	}
	if cnt == 0 {
		return ErrMetaKeyNotFound
	}
	return mm.redisCli.Incr(ctx, redisMetasVersionKey).Err()
}

func (mm *MetaManager) ListPusherByNamespace(wantedPool, wantedNamespace string) map[string]Meta {
	pushGroupMetas := make(map[string]Meta)
	for key, meta := range mm.metas {
		pool, ns, group, err := mm.splitKey(key)
		if err != nil {
			continue
		}
		if wantedPool == pool && wantedNamespace == ns {
			pushGroupMetas[group] = *meta
		}
	}
	return pushGroupMetas
}

func (mm *MetaManager) Dump() map[string]map[string][]string {
	pushGroups := make(map[string]map[string][]string)
	for key := range mm.metas {
		pool, ns, group, err := mm.splitKey(key)
		if err != nil {
			continue
		}
		if _, ok := pushGroups[pool]; !ok {
			pushGroups[pool] = make(map[string][]string, 0)
		}
		if _, ok := pushGroups[pool][ns]; !ok {
			pushGroups[pool][ns] = make([]string, 0)
		}
		pushGroups[pool][ns] = append(pushGroups[pool][ns], group)
	}
	return pushGroups
}

func (meta *Meta) Validate() error {
	if len(meta.Queues) <= 0 || len(meta.Queues) > maxTopicNum {
		return fmt.Errorf("the number of queues should be between 1 and %d", maxTopicNum)
	}
	if meta.Workers <= 0 || meta.Workers > maxWorkerNum {
		return fmt.Errorf("workers should be between 1 and %d", maxWorkerNum)
	}
	if meta.Timeout <= 0 || meta.Timeout > 3600 {
		return errors.New("timeout should be between 1 and 3600")
	}
	_, err := url.ParseRequestURI(meta.Endpoint)
	return err
}
