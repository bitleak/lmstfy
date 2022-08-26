package pumper

import (
	"time"

	lock2 "github.com/bitleak/lmstfy/datamanager/lock"
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

	if err := p.lock.Acquire(); err == nil {
		isLeader = true
	}

	defer func() {
		if isLeader {
			if _, err := p.lock.Release(); err != nil {
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
				continueLoop = fn()
			}
		case <-electTicker.C:
			if isLeader {
				extendLeaseOK, err := p.lock.ExtendLease()
				if !extendLeaseOK || err != nil {
					isLeader = false
				}
			} else {
				if err := p.lock.Acquire(); err == nil {
					// Become Leader
					isLeader = true
					return
				}
			}
		case <-p.shutdown:
			return
		}
	}
}

func (p *Default) Shutdown() {
	close(p.shutdown)
}
