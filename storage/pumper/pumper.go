package pumper

import (
	"time"

	"github.com/bitleak/lmstfy/log"
	lock2 "github.com/bitleak/lmstfy/storage/lock"
)

type Pumper interface {
	Loop(fn func() bool)
	Shutdown()
}

type Default struct {
	lock     lock2.Lock
	interval time.Duration
	shutdown chan struct{}
}

func NewDefault(lock lock2.Lock, interval time.Duration) *Default {
	return &Default{
		lock:     lock,
		interval: interval,
		shutdown: make(chan struct{}),
	}
}

func (p *Default) Loop(fn func() bool) {
	isLeader := false

	logger := log.Get()
	if err := p.lock.Acquire(); err == nil {
		isLeader = true
		logger.Info("Acquired the pumper lock, I'm leader now")
	} else {
		logger.WithError(err).Info("Lost the pumper lock")
	}

	defer func() {
		if isLeader {
			if _, err := p.lock.Release(); err != nil {
				logger.WithError(err).Error("Failed to release the pumper lock")
			}
		}
	}()

	pumpTicker := time.NewTicker(p.interval)
	electTicker := time.NewTicker(p.lock.Expiry() / 3)
	for {
		select {
		case <-pumpTicker.C:
			continueLoop := true
			for isLeader && continueLoop {
				select {
				case <-p.shutdown:
					return
				default:
				}
				continueLoop = fn()
			}
		case <-electTicker.C:
			if isLeader {
				extendLeaseOK, err := p.lock.ExtendLease()
				if !extendLeaseOK || err != nil {
					isLeader = false
					logger.WithError(err).Error("Failed to extend lease")
				}
			} else {
				if err := p.lock.Acquire(); err == nil {
					// Become Leader
					isLeader = true
					logger.Info("Acquired the pumper lock, I'm leader now")
					continue
				}
			}
		case <-p.shutdown:
			if isLeader {
				logger.Info("The pumper was shutdown, will release the pumper lock")
			}
			return
		}
	}
}

func (p *Default) Shutdown() {
	close(p.shutdown)
}
