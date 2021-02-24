package push

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/helper"
)

type Manager struct {
	*MetaManager
	pushGroups sync.Map
	logger     *logrus.Logger
}

func NewManger(redisCli *redis.Client, updateInterval time.Duration, logger *logrus.Logger) (*Manager, error) {
	var err error
	manager := new(Manager)
	manager.logger = logger
	manager.MetaManager, err = newMetaManager(
		redisCli,
		updateInterval,
		logger,
		manager.onCreated,
		manager.onUpdated,
		manager.onDeleted)
	if err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *Manager) onCreated(pool, ns, group string, meta *Meta) {
	key := m.buildKey(pool, ns, group)
	if v, ok := m.pushGroups.Load(key); ok {
		v.(*Pusher).stop()
		m.pushGroups.Delete(key)
	}
	pusher := newPusher(pool, ns, group, meta, m.logger)
	if err := pusher.start(); err != nil {
		m.logger.WithFields(logrus.Fields{
			"pool":  pool,
			"ns":    ns,
			"group": group,
			"meta":  meta,
		}).Error("Failed to start the push group")
		return
	}
	m.logger.WithFields(logrus.Fields{
		"ns":    ns,
		"group": group,
	}).Info("Success to create the push group")
	m.pushGroups.Store(key, pusher)
}

func (m *Manager) onUpdated(pool, ns, group string, newMeta *Meta) {
	key := m.buildKey(pool, ns, group)
	v, ok := m.pushGroups.Load(key)
	if !ok {
		m.onCreated(pool, ns, group, newMeta)
		return
	}
	pusher := v.(*Pusher)
	pusher.Meta = newMeta
	if err := pusher.restart(); err != nil {
		m.logger.WithFields(logrus.Fields{
			"pool":  pool,
			"ns":    ns,
			"group": group,
			"meta":  newMeta,
		}).Error("Failed to restart the push group")
		return
	}
	m.logger.WithFields(logrus.Fields{
		"pool":  pool,
		"ns":    ns,
		"group": group,
	}).Info("Success to update the push group")
	m.pushGroups.Store(key, pusher)
}

func (m *Manager) onDeleted(pool, ns, group string) {
	key := m.buildKey(pool, ns, group)
	if v, ok := m.pushGroups.Load(key); ok {
		if err := v.(*Pusher).stop(); err != nil {
			m.logger.WithFields(logrus.Fields{
				"pool":  pool,
				"ns":    ns,
				"group": group,
			}).Error("Failed to stop the push group")
			return
		}
		m.logger.WithFields(logrus.Fields{
			"pool":  pool,
			"ns":    ns,
			"group": group,
		}).Info("Success to delete the push group")
		m.pushGroups.Delete(key)
	}
}

// SetCallbacks used to set custom callback when meta was changed.
// e.g. we want to use empty callback function to fasten test cases
func (m *Manager) SetCallbacks(onCreated onCreatedFunc,
	onUpdated onUpdatedFunc,
	onDeleted onDeletedFunc) {
	m.MetaManager.onCreated = onCreated
	m.MetaManager.onUpdated = onUpdated
	m.MetaManager.onDeleted = onDeleted
}

var _manager *Manager

func Setup(conf *config.Config, updateInterval time.Duration, logger *logrus.Logger) error {
	var err error
	redisConf := conf.AdminRedis
	cli := helper.NewRedisClient(&redisConf, nil)
	if cli.Ping(dummyCtx).Err() != nil {
		return fmt.Errorf("can not connect to admin redis: %s", err.Error())
	}
	setupMetrics()
	_manager, err = NewManger(cli, updateInterval, logger)
	return err
}

func GetManager() *Manager {
	return _manager
}

func Shutdown() {
	_manager.Close()
	_manager.pushGroups.Range(func(key, value interface{}) bool {
		value.(*Pusher).stop()
		return true
	})
}
