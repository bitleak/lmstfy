package push

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/engine"
)

type Pusher struct {
	*Meta
	Pool            string
	Namespace       string
	Queue           string
	logger          *logrus.Logger
	jobChan         chan engine.Job
	wg              sync.WaitGroup
	consumerStopper chan struct{}
	pusherStopper   chan struct{}
	httpClient      *http.Client
}

func (p *Pusher) start() error {
	p.consumerStopper = make(chan struct{})
	p.pusherStopper = make(chan struct{})
	p.jobChan = make(chan engine.Job)
	p.wg = sync.WaitGroup{}
	p.httpClient = &http.Client{
		Timeout: time.Duration(p.Timeout) * time.Second,
	}
	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"queue": p.Queue,
	}).Info("Start the pusher")
	// Todo: check pool exist
	go p.startConsume()
	for i := 0; i < p.Workers; i++ {
		p.logger.WithFields(logrus.Fields{
			"pool":        p.Pool,
			"ns":          p.Namespace,
			"queue":       p.Queue,
			"process_num": i,
		}).Info("Pusher start push")
		p.wg.Add(1)
		go p.startPush(i)
	}
	return nil
}

func (p *Pusher) startConsume() {
	defer func() {
		if err := recover(); err != nil {
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"queue": p.Queue,
				"error": err,
			}).Error("Pusher consume panic")
		}
	}()
	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"queue": p.Queue,
	}).Info("Pusher start consume")
	for {
		select {
		case <-p.consumerStopper:
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"queue": p.Queue,
			}).Info("Pusher stop consume")
			close(p.pusherStopper)
			return
		default:
		}
		e := engine.GetEngine(p.Pool)
		// consume
		job, err := e.ConsumeByPush(p.Namespace, p.Queue, p.Timeout, 10)
		if err != nil {
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"queue": p.Queue,
				"error": err,
			}).Error("Failed to consume")
		}
		p.jobChan <- job
	}

}

func (p *Pusher) startPush(num int) {
	defer func() {
		if err := recover(); err != nil {
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"queue": p.Queue,
				"error": err,
			}).Error("Pusher push panic")
		}
	}()
	defer p.wg.Done()
	for {
		select {
		case job := <-p.jobChan:
			if job != nil {
				p.push(job)
			}
		case <-p.pusherStopper:
			p.logger.WithFields(logrus.Fields{
				"pool":        p.Pool,
				"ns":          p.Namespace,
				"queue":       p.Queue,
				"process_num": num,
			}).Info("Pusher stop push")
			return
		}
	}
}

func (p *Pusher) push(job engine.Job) {
	req, err := http.NewRequest(http.MethodPost, p.Endpoint, bytes.NewReader(job.Body()))
	if err != nil {
		p.logger.WithFields(logrus.Fields{
			"error":  err,
			"pusher": p,
		}).Error("Pusher new request error")
		return
	}
	req.Header.Add("namespace", job.Namespace())
	req.Header.Add("queue", job.Queue())
	req.Header.Add("job_id", job.ID())
	req.Header.Add("ttl", strconv.FormatUint(uint64(job.TTL()), 10))
	req.Header.Add("elapsed_ms", strconv.FormatInt(job.ElapsedMS(), 10))
	req.Header.Add("remain_tries", strconv.FormatUint(uint64(job.Tries()), 10))
	resp, err := p.httpClient.Do(req)
	if err != nil {
		p.logger.WithFields(logrus.Fields{
			"error":  err,
			"pusher": p,
		}).Debug("Pusher do request error")
		return
	}
	ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		e := engine.GetEngine(p.Pool)
		err := e.Delete(p.Namespace, p.Queue, job.ID())
		if err != nil {
			p.logger.WithFields(logrus.Fields{
				"error":  err,
				"pusher": p,
			}).Error("Pusher delete job error")
			return
		}
	}
	return
}

func (p *Pusher) stop() error {
	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"queue": p.Queue,
	}).Info("Stop the pusher")
	close(p.consumerStopper)
	p.wg.Wait()
	close(p.jobChan)
	return nil
}

func (p *Pusher) restart() error {
	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"queue": p.Queue,
	}).Info("Restart the pusher")
	p.stop()
	p.start()
	return nil
}
