package push

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/engine"
)

type Pusher struct {
	*Meta
	Pool       string
	Namespace  string
	Group      string
	logger     *logrus.Logger
	httpClient *http.Client

	consumerWg      sync.WaitGroup
	workerWg        sync.WaitGroup
	jobCh           chan engine.Job
	stopCh          chan struct{}
	restartWorkerCh chan struct{}
}

func newPusher(pool, ns, group string, meta *Meta, logger *logrus.Logger) *Pusher {
	pusher := &Pusher{
		Meta:      meta,
		Pool:      pool,
		Namespace: ns,
		Group:     group,
		logger:    logger,

		jobCh:           make(chan engine.Job, maxWorkerNum),
		stopCh:          make(chan struct{}),
		restartWorkerCh: make(chan struct{}),
		httpClient:      newHttpClient(meta),
	}
	go pusher.pollQueues()
	return pusher
}

func (p *Pusher) start() error {
	for i := 0; i < p.Workers; i++ {
		p.workerWg.Add(1)
		go p.startWorker(i)
	}
	return nil
}

func (p *Pusher) pollQueues() {
	defer func() {
		if err := recover(); err != nil {
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"group": p.Group,
				"error": err,
			}).Error("Panic in poll queues")
		}
	}()
	p.consumerWg.Add(1)
	defer p.consumerWg.Done()

	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"group": p.Group,
	}).Info("Start polling queues")

	engine := engine.GetEngine(p.Pool)
	for {
		now := time.Now()
		job, err := engine.ConsumeMultiWithFrozenTries(p.Namespace, p.Meta.Queues, p.Timeout, 3)
		if err != nil {
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"group": p.Group,
				"error": err,
			}).Error("Failed to consume")
		}
		select {
		case p.jobCh <- job:
			metrics.ConsumeLatencies.WithLabelValues(
				p.Pool,
				p.Namespace,
				p.Group).Observe(float64(time.Since(now).Milliseconds()))
			/* do nothing */
		case <-p.stopCh:
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"group": p.Group,
			}).Info("Shutdown polling queues while the stop signal was received")
			return
		}
	}
}

func (p *Pusher) startWorker(num int) {
	defer func() {
		if err := recover(); err != nil {
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"group": p.Group,
				"error": err,
			}).Error("Panic in worker")
		}
	}()
	defer p.workerWg.Done()
	p.logger.WithFields(logrus.Fields{
		"pool":        p.Pool,
		"ns":          p.Namespace,
		"group":       p.Group,
		"process_num": num,
	}).Info("Start the worker")
	for {
		select {
		case job := <-p.jobCh:
			if job != nil {
				if err := p.sendJobToUser(job); err != nil {
					p.logger.WithFields(logrus.Fields{
						"pool":   p.Pool,
						"ns":     p.Namespace,
						"group":  p.Group,
						"job_id": job.ID(),
						"err":    err.Error(),
					}).Debug("Failed to send the data to user endpoint")
				}
			}
		case <-p.restartWorkerCh:
			p.logger.WithFields(logrus.Fields{
				"pool":        p.Pool,
				"ns":          p.Namespace,
				"group":       p.Group,
				"process_num": num,
			}).Info("Shutdown the worker while the restart signal was received")
			return
		case <-p.stopCh:
			p.logger.WithFields(logrus.Fields{
				"pool":        p.Pool,
				"ns":          p.Namespace,
				"group":       p.Group,
				"process_num": num,
			}).Info("Shutdown the worker while the stop signal was received")
			return
		}
	}
}

func (p *Pusher) sendJobToUser(job engine.Job) error {
	var statusCode int
	defer func(t time.Time) {
		metrics.PushLatencies.WithLabelValues(
			p.Pool,
			p.Namespace,
			p.Group).Observe(float64(time.Since(t).Milliseconds()))

		metrics.PushHTTPCodes.WithLabelValues(
			p.Pool,
			p.Namespace,
			p.Group,
			strconv.Itoa(statusCode),
		).Inc()
	}(time.Now())
	jobBytes, _ := job.MarshalText()
	req, err := http.NewRequest(http.MethodPost, p.Endpoint, bytes.NewReader(jobBytes))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	statusCode = resp.StatusCode
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		e := engine.GetEngine(p.Pool)
		return e.Delete(p.Namespace, p.Group, job.ID())
	}
	return fmt.Errorf("got unexpected http code: %d", resp.StatusCode)
}

func (p *Pusher) stop() error {
	close(p.stopCh)
	p.consumerWg.Wait()
	p.workerWg.Wait()
	close(p.jobCh)
	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"group": p.Group,
	}).Info("Shutdown the pusher")
	return nil
}

func (p *Pusher) restart() error {
	close(p.restartWorkerCh)
	p.workerWg.Wait()
	p.restartWorkerCh = make(chan struct{})
	p.start()
	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"group": p.Group,
	}).Info("Restart the pusher")
	return nil
}
func newHttpClient(meta *Meta) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 20 * time.Second,
		}).DialContext,
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 20,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     60 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(meta.Timeout) * time.Second,
	}

	return client
}
