package push

import (
	"errors"
	"sync"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/helper"
)

type Manager struct {
	*MetaManager
	pushers sync.Map
	logger  *logrus.Logger
}

func NewManger(redisCli *redis.Client, logger *logrus.Logger) (*Manager, error) {
	var err error
	manager := new(Manager)
	manager.logger = logger
	manager.MetaManager, err = newMetaManager(
		redisCli,
		logger,
		manager.onCreated,
		manager.onUpdated,
		manager.onDeleted)
	if err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *Manager) onCreated(ns, queue string, meta *Meta) {
	key := m.buildKey(ns, queue)
	if v, ok := m.pushers.Load(key); ok {
		v.(*Pusher).stop()
		m.pushers.Delete(key)
	}
	pusher := &Pusher{
		Namespace: ns,
		Queue:     queue,
		Meta:      meta,
		logger:    m.logger,
	}
	if err := pusher.start(); err != nil {
		m.logger.WithFields(logrus.Fields{
			"ns":    ns,
			"queue": queue,
			"meta":  meta,
		}).Error("Failed to start the pusher")
		return
	}
	m.logger.WithFields(logrus.Fields{
		"ns":    ns,
		"queue": queue,
	}).Info("Success to create the new pusher")
	m.pushers.Store(key, pusher)
}

func (m *Manager) onUpdated(ns, queue string, newMeta *Meta) {
	key := m.buildKey(ns, queue)
	v, ok := m.pushers.Load(key)
	if !ok {
		m.onCreated(ns, queue, newMeta)
		return
	}
	pusher := v.(*Pusher)
	pusher.Meta = newMeta
	if err := pusher.restart(); err != nil {
		m.logger.WithFields(logrus.Fields{
			"ns":    ns,
			"queue": queue,
			"meta":  newMeta,
		}).Error("Failed to restart the Pusher")
		return
	}
	m.logger.WithFields(logrus.Fields{
		"ns":    ns,
		"queue": queue,
	}).Info("Success to update the pusher")
	m.pushers.Store(key, pusher)
}

func (m *Manager) onDeleted(ns, queue string) {
	key := m.buildKey(ns, queue)
	if v, ok := m.pushers.Load(key); ok {
		if err := v.(*Pusher).stop(); err != nil {
			m.logger.WithFields(logrus.Fields{
				"ns":    ns,
				"queue": queue,
			}).Error("Failed to stop the Pusher")
			return
		}
		m.logger.WithFields(logrus.Fields{
			"ns":    ns,
			"queue": queue,
		}).Info("Success to delete the pusher")
		m.pushers.Delete(key)
	}
}

var _manager *Manager

func Setup(conf *config.Config, logger *logrus.Logger) error {
	var err error
	redisConf := conf.AdminRedis
	cli := helper.NewRedisClient(&redisConf, nil)
	if cli.Ping().Err() != nil {
		return errors.New("can not connect to admin redis")
	}
	_manager, err = NewManger(cli, logger)
	return err
}

func GetManager() *Manager {
	return _manager
}
