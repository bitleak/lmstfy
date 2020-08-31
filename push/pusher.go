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
	Pool       string
	Namespace  string
	Queue      string
	logger     *logrus.Logger
	httpClient *http.Client

	wg              sync.WaitGroup
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
	// TODO: check pool exist
	go pusher.pollQueue()
	return pusher
}

func (p *Pusher) start() error {
	for i := 0; i < p.Workers; i++ {
		p.wg.Add(1)
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
	defer p.wg.Done()

	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"queue": p.Queue,
	}).Info("Start polling queue")

	for {
		e := engine.GetEngine(p.Pool)
		job, err := e.ConsumeByPush(p.Namespace, p.Queue, p.Timeout, 10)
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
	defer p.wg.Done()

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
	req, err := http.NewRequest(http.MethodPost, p.Endpoint, bytes.NewReader(job.Body()))
	if err != nil {
		return err
	}
	req.Header.Add("namespace", job.Namespace())
	req.Header.Add("queue", job.Queue())
	req.Header.Add("job_id", job.ID())
	req.Header.Add("ttl", strconv.FormatUint(uint64(job.TTL()), 10))
	req.Header.Add("elapsed_ms", strconv.FormatInt(job.ElapsedMS(), 10))
	req.Header.Add("remain_tries", strconv.FormatUint(uint64(job.Tries()), 10))
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		e := engine.GetEngine(p.Pool)
		err := e.Delete(p.Namespace, p.Queue, job.ID())
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pusher) stop() error {
	close(p.stopCh)
	p.wg.Wait()
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
	p.wg.Wait()
	p.start()
	p.logger.WithFields(logrus.Fields{
		"pool":  p.Pool,
		"ns":    p.Namespace,
		"queue": p.Queue,
	}).Info("Restart the pusher")
	return nil
}
