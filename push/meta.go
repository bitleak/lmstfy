package push

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/go-cmp/cmp"
	"github.com/sirupsen/logrus"
)

const redisMetasKey = "__push_metas__"
const redisMetasVersionKey = "__push_metas_version__"
const metaKeyPrefix = "pm"
const maxWorkerNum = 64

var ErrInvalidKey = errors.New("invalid push meta key")
var ErrMetaKeyExists = errors.New("the meta key has already exists")
var ErrMetaKeyNotFound = errors.New("the meta key was not found")

type Meta struct {
	Endpoint string `json:"endpoint"`
	Workers  int    `json:"workers"`
	Timeout  uint32 `json:"timeout"`
}

type onCreatedFunc func(pool, ns, queue string, meta *Meta)
type onUpdatedFunc func(pool, ns, queue string, newMeta *Meta)
type onDeletedFunc func(pool, ns, queue string)

type MetaManager struct {
	redisCli           *redis.Client
	metas              map[string]*Meta
	logger             *logrus.Logger
	latestMetasVersion int64

	// callback functions
	onCreated onCreatedFunc
	onUpdated onUpdatedFunc
	onDeleted onDeletedFunc

	stopCh chan struct{}
}

func newMetaManager(
	redisCli *redis.Client,
	logger *logrus.Logger,
	onCreated onCreatedFunc,
	onUpdated onUpdatedFunc,
	onDeleted onDeletedFunc) (*MetaManager, error) {
	latestMetasVersion, err := redisCli.Get(redisMetasVersionKey).Int64()
	if err == redis.Nil {
		latestMetasVersion, err = redisCli.Incr(redisMetasVersionKey).Result()
	}
	mm := &MetaManager{
		redisCli:           redisCli,
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

func (mm *MetaManager) buildKey(pool, ns, queue string) string {
	return fmt.Sprintf("%s/%s/%s/%s", metaKeyPrefix, pool, ns, queue)
}

func (mm *MetaManager) splitKey(key string) (string, string, string, error) {
	fields := strings.Split(key, "/")
	if len(fields) != 4 || fields[0] != metaKeyPrefix {
		return "", "", "", ErrInvalidKey
	}
	return fields[1], fields[2], fields[3], nil
}

func (mm *MetaManager) updateMetas() error {
	vals, err := mm.redisCli.HGetAll(redisMetasKey).Result()
	if err != nil {
		return err
	}
	newMetas := make(map[string]*Meta, len(vals))
	for key, meta := range vals {
		newMeta := new(Meta)
		pool, ns, queue, err := mm.splitKey(key)
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
				"queue": queue,
				"err":   err,
			}).Warn("Failed to marshal the pusher's meta")
			continue
		}
		if oldMeta, ok := mm.metas[key]; ok {
			if !cmp.Equal(newMeta, oldMeta) { // the meta was modified
				mm.onUpdated(pool, ns, queue, newMeta)
			}
		} else { // new meta was created
			mm.onCreated(pool, ns, queue, newMeta)
		}
		newMetas[key] = newMeta
	}
	for oldKey := range mm.metas {
		pool, ns, queue, err := mm.splitKey(oldKey)
		if err != nil {
			mm.logger.WithFields(logrus.Fields{
				"key": oldKey,
				"err": err,
			}).Debug("Invalid pusher's key")
			continue
		}
		if _, ok := newMetas[oldKey]; !ok {
			// the meta was deleted
			mm.onDeleted(pool, ns, queue)
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
	ticker := time.NewTicker(3 * time.Second)
	mm.updateMetas()
	for {
		select {
		case <-ticker.C:
			latestMetasVersion, err := mm.redisCli.Get(redisMetasVersionKey).Int64()
			if err != nil {
				mm.logger.WithFields(logrus.Fields{
					"err": err,
				}).Warn("Failed to fetch the metas version key")
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
			mm.logger.Info("meta manager would be exited while the stop signal was received")
			return
		}
	}
}

func (mm *MetaManager) GetFromRemote(pool, ns, queue string) (*Meta, error) {
	key := mm.buildKey(pool, ns, queue)
	metaStr, err := mm.redisCli.HGet(redisMetasKey, key).Result()
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

func (mm *MetaManager) Get(pool, ns, queue string) *Meta {
	key := mm.buildKey(pool, ns, queue)
	if pushMeta, ok := mm.metas[key]; ok {
		return pushMeta
	}
	return nil
}

func (mm *MetaManager) Create(pool, ns, queue string, meta *Meta) error {
	key := mm.buildKey(pool, ns, queue)
	bytes, _ := json.Marshal(meta)
	ok, err := mm.redisCli.HSetNX(redisMetasKey, key, string(bytes)).Result()
	if err != nil {
		return err
	}
	if !ok {
		return ErrMetaKeyExists
	}
	mm.redisCli.Incr(redisMetasVersionKey)
	return nil
}

func (mm *MetaManager) Update(pool, ns, queue string, meta *Meta) error {
	key := mm.buildKey(pool, ns, queue)
	bytes, _ := json.Marshal(meta)
	_, err := mm.redisCli.HSet(redisMetasKey, key, string(bytes)).Result()
	if err != nil {
		return err
	}
	mm.redisCli.Incr(redisMetasVersionKey)
	return nil
}

func (mm *MetaManager) Delete(pool, ns, queue string) error {
	key := mm.buildKey(pool, ns, queue)
	cnt, err := mm.redisCli.HDel(redisMetasKey, key).Result()
	if err != nil {
		return err
	}
	if cnt == 0 {
		return ErrMetaKeyNotFound
	}
	mm.redisCli.Incr(redisMetasVersionKey)
	return nil
}

func (mm *MetaManager) ListPusherByNamespace(wantedPool, wantedNamespace string) map[string]Meta {
	queueMetas := make(map[string]Meta)
	for key, meta := range mm.metas {
		pool, ns, queue, err := mm.splitKey(key)
		if err != nil {
			continue
		}
		if wantedPool == pool && wantedNamespace == ns {
			queueMetas[queue] = *meta
		}
	}
	return queueMetas
}

func (mm *MetaManager) Dump() map[string]map[string][]string {
	pushQueues := make(map[string]map[string][]string)
	for key := range mm.metas {
		pool, ns, queue, err := mm.splitKey(key)
		if err != nil {
			continue
		}
		if _, ok := pushQueues[pool]; !ok {
			pushQueues[pool] = make(map[string][]string, 0)
		}
		if _, ok := pushQueues[pool][ns]; !ok {
			pushQueues[pool][ns] = make([]string, 0)
		}
		pushQueues[pool][ns] = append(pushQueues[pool][ns], queue)
	}
	return pushQueues
}

func (meta *Meta) Validate() error {
	if meta.Workers <= 0 || meta.Workers > maxWorkerNum {
		return errors.New(fmt.Sprintf("workers should be between 1 and %d",maxWorkerNum))
	}
	if meta.Timeout <= 0 || meta.Timeout > 3600 {
		return errors.New("timeout should be between 1 and 3600")
	}
	_, err := url.ParseRequestURI(meta.Endpoint)
	return err
}
