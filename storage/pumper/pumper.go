package pumper

import (
	"sync"
	"time"

	"github.com/bitleak/lmstfy/log"
	redislock "github.com/bitleak/lmstfy/storage/lock"
	"go.uber.org/atomic"
)

type Pumper interface {
	Run(fn func() bool)
	Shutdown()
}

type Default struct {
	isLeader atomic.Bool
	lock     redislock.Lock
	interval time.Duration

	wg       sync.WaitGroup
	shutdown chan struct{}
}

func NewDefault(lock redislock.Lock, interval time.Duration) *Default {
	p := &Default{
		lock:     lock,
		interval: interval,
		shutdown: make(chan struct{}),
	}
	p.isLeader.Store(false)

	logger := log.Get().WithField("lock_name", p.lock.Name())
	if err := p.lock.Acquire(); err == nil {
		p.isLeader.Store(true)
		logger.Info("Acquired the pumper lock, I'm leader now")
	} else {
		logger.WithError(err).Info("Lost the pumper lock")
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.keepalive()
	}()
	return p
}

func (p *Default) keepalive() {
	logger := log.Get().WithField("lock_name", p.lock.Name())

	keepAliveTicker := time.NewTicker(p.lock.Expiry() / 3)
	for {
		select {
		case <-keepAliveTicker.C:
			if p.isLeader.Load() {
				extendLeaseOK, err := p.lock.ExtendLease()
				if !extendLeaseOK || err != nil {
					p.isLeader.Store(false)
					logger.WithError(err).Error("Failed to extend lease")
				}
			} else {
				if err := p.lock.Acquire(); err == nil {
					// Become Leader
					p.isLeader.Store(true)
					logger.Info("Become leader now")
					continue
				}
			}
		case <-p.shutdown:
			logger.Info("Keep alive will be exited because the pumper was shutdown")
			return
		}
	}
}

func (p *Default) Run(fn func() bool) {
	logger := log.Get().WithField("lock_name", p.lock.Name())

	defer func() {
		if p.isLeader.Load() {
			if _, err := p.lock.Release(); err != nil {
				logger.WithError(err).Error("Failed to release the pumper lock")
			}
		}
	}()

	pumpTicker := time.NewTicker(p.interval)
	for {
		select {
		case <-pumpTicker.C:
			continueLoop := true
			for p.isLeader.Load() && continueLoop {
				select {
				case <-p.shutdown:
					logger.Info("The pumper was shutdown, will release the pumper lock")
					return
				default:
				}
				continueLoop = fn()
			}
		case <-p.shutdown:
			logger.Info("The pumper was shutdown, will release the pumper lock")
			return
		}
	}
}

func (p *Default) Shutdown() {
	close(p.shutdown)
	p.wg.Wait()
}
