package push

import (
	"bytes"
	"fmt"
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
	Pool       string
	Namespace  string
	Queue      string
	logger     *logrus.Logger
	httpClient *http.Client

	consumerWg      sync.WaitGroup
	workerWg        sync.WaitGroup
	jobCh           chan engine.Job
	stopCh          chan struct{}
	restartWorkerCh chan struct{}
}

func newPusher(pool, ns, queue string, meta *Meta, logger *logrus.Logger) *Pusher {
	pusher := &Pusher{
		Meta:      meta,
		Pool:      pool,
		Namespace: ns,
		Queue:     queue,
		logger:    logger,

		jobCh:           make(chan engine.Job),
		stopCh:          make(chan struct{}),
		restartWorkerCh: make(chan struct{}),
		httpClient:      &http.Client{Timeout: time.Duration(meta.Timeout) * time.Second},
	}
	go pusher.pollQueue()
	return pusher
}

func (p *Pusher) start() error {
	for i := 0; i < p.Workers; i++ {
		p.workerWg.Add(1)
		go p.startWorker(i)
	}
	return nil
}

func (p *Pusher) pollQueue() {
	defer func() {
		if err := recover(); err != nil {
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"queue": p.Queue,
				"error": err,
			}).Error("Panic in poll queue")
		}
	}()
	p.consumerWg.Add(1)
	defer p.consumerWg.Done()

	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"queue": p.Queue,
	}).Info("Start polling queue")

	engine := engine.GetEngine(p.Pool)
	for {
		now := time.Now()
		job, err := engine.ConsumeByPush(p.Namespace, p.Queue, p.Timeout, 3)
		if err != nil {
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"queue": p.Queue,
				"error": err,
			}).Error("Failed to consume")
		}
		select {
		case p.jobCh <- job:
			metrics.ConsumeLatencies.WithLabelValues(
				p.Pool,
				p.Namespace,
				p.Queue).Observe(time.Since(now).Seconds() * 1000)
			/* do nothing */
		case <-p.stopCh:
			p.logger.WithFields(logrus.Fields{
				"pool":  p.Pool,
				"ns":    p.Namespace,
				"queue": p.Queue,
			}).Info("Stop polling queue while the stop signal was received")
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
				"queue": p.Queue,
				"error": err,
			}).Error("Panic in worker")
		}
	}()
	defer p.workerWg.Done()
	p.logger.WithFields(logrus.Fields{
		"pool":        p.Pool,
		"ns":          p.Namespace,
		"queue":       p.Queue,
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
						"queue":  p.Queue,
						"job_id": job.ID(),
						"err":    err.Error(),
					}).Warn("Failed to send the data to user endpoint")
				}
			}
		case <-p.restartWorkerCh:
			p.logger.WithFields(logrus.Fields{
				"pool":        p.Pool,
				"ns":          p.Namespace,
				"queue":       p.Queue,
				"process_num": num,
			}).Info("Stop the worker while the restart signal was received")
			return
		case <-p.stopCh:
			p.logger.WithFields(logrus.Fields{
				"pool":        p.Pool,
				"ns":          p.Namespace,
				"queue":       p.Queue,
				"process_num": num,
			}).Info("Stop the worker while the stop signal was received")
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
			p.Queue).Observe(time.Since(t).Seconds() * 1000)
		metrics.PushHTTPCodes.WithLabelValues(
			p.Pool,
			p.Namespace,
			p.Queue,
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
		return e.Delete(p.Namespace, p.Queue, job.ID())
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
		"queue": p.Queue,
	}).Info("Stop the pusher")
	return nil
}

func (p *Pusher) restart() error {
	close(p.restartWorkerCh)
	p.restartWorkerCh = make(chan struct{})
	p.workerWg.Wait()
	p.start()
	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"queue": p.Queue,
	}).Info("Restart the pusher")
	return nil
}
