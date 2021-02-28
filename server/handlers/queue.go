package handlers

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/bitleak/lmstfy/engine"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

const (
	maxBatchConsumeSize = 100
	maxBulkPublishSize  = 64
)

// PUT /:namespace/:queue
// @query:
//  - delay: uint32
//  - ttl:   uint32
//  - tries: uint16
func Publish(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")
	jobID := c.Param("job_id")

	if jobID != "" {
		// delete job whatever other publish parameters
		if err := e.Delete(namespace, queue, jobID); err != nil {
			logger.WithFields(logrus.Fields{
				"err":       err,
				"namespace": namespace,
				"queue":     queue,
				"job_id":    jobID,
			}).Error("Failed to delete")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
	}

	delaySecondStr := c.DefaultQuery("delay", DefaultDelay)
	delaySecond, err := strconv.ParseUint(delaySecondStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid delay"})
		return
	}

	ttlSecondStr := c.DefaultQuery("ttl", DefaultTTL)
	ttlSecond, err := strconv.ParseUint(ttlSecondStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ttl"})
		return
	}

	// NOTE: ttlSecond == 0 means forever, so it's always longer than any delay
	if ttlSecond > 0 && ttlSecond < delaySecond {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ttl is shorter than delay"})
		return
	}

	triesStr := c.DefaultQuery("tries", DefaultTries)
	tries, err := strconv.ParseUint(triesStr, 10, 16)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid tries"})
		return
	}
	if tries == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tries shouldn't be zero"})
		return
	}

	body, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return
	}
	if len(body) > math.MaxUint16 { // Larger than 64 KB
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "body too large"})
		return
	}

	jobID, err = e.Publish(namespace, queue, body, uint32(ttlSecond), uint32(delaySecond), uint16(tries))
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":       err,
			"namespace": namespace,
			"queue":     queue,
			"job_id":    jobID,
			"delay":     delaySecond,
			"ttl":       ttlSecond,
			"tries":     tries,
		}).Error("Failed to publish")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	logger.WithFields(logrus.Fields{
		"namespace": namespace,
		"queue":     queue,
		"job_id":    jobID,
		"delay":     delaySecond,
		"ttl":       ttlSecond,
		"tries":     tries,
	}).Debug("Job published")
	c.JSON(http.StatusCreated, gin.H{"msg": "published", "job_id": jobID})
}

// PUT /:namespace/:queue/bulk
// @query:
//  - delay: uint32
//  - ttl:   uint32
//  - tries: uint16
func PublishBulk(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")

	delaySecondStr := c.DefaultQuery("delay", DefaultDelay)
	delaySecond, err := strconv.ParseUint(delaySecondStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid delay"})
		return
	}

	ttlSecondStr := c.DefaultQuery("ttl", DefaultTTL)
	ttlSecond, err := strconv.ParseUint(ttlSecondStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ttl"})
		return
	}

	// NOTE: ttlSecond == 0 means forever, so it's always longer than any delay
	if ttlSecond > 0 && ttlSecond < delaySecond {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ttl is shorter than delay"})
		return
	}

	triesStr := c.DefaultQuery("tries", DefaultTries)
	tries, err := strconv.ParseUint(triesStr, 10, 16)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid tries"})
		return
	}
	if tries == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tries shouldn't be zero"})
		return
	}

	body, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return
	}
	jobs := make([]json.RawMessage, 0)
	if err := json.Unmarshal(body, &jobs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body should be an array of objects"})
		return
	}
	count := len(jobs)
	if count == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no jobs"})
		return
	}
	if count > maxBulkPublishSize {
		c.JSON(http.StatusBadRequest, gin.H{"error": "too many jobs"})
		return
	}
	for _, job := range jobs {
		if len(job) > math.MaxUint16 { // Larger than 64 KB
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "job too large"})
			return
		}
	}

	jobIDs := make([]string, 0)
	for _, job := range jobs {
		jobID, err := e.Publish(namespace, queue, job, uint32(ttlSecond), uint32(delaySecond), uint16(tries))
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err":       err,
				"namespace": namespace,
				"queue":     queue,
				"job_id":    jobID,
				"delay":     delaySecond,
				"ttl":       ttlSecond,
				"tries":     tries,
			}).Error("Failed to publish")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		logger.WithFields(logrus.Fields{
			"namespace": namespace,
			"queue":     queue,
			"job_id":    jobID,
			"delay":     delaySecond,
			"ttl":       ttlSecond,
			"tries":     tries,
		}).Debug("Job published")
		jobIDs = append(jobIDs, jobID)
	}
	c.JSON(http.StatusCreated, gin.H{"msg": "published", "job_ids": jobIDs})
}

// GET /:namespace/:queue[,:queue]*
// @query:
//  - ttr:     uint32
//  - timeout: uint32
//  - count:   uint32
// NOTE: according to RFC3986, the URL path part can contain comma(",") ,
// so I decide to use "," as the separator of queue names
func Consume(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queues := c.Param("queue") // NOTE: param name should be `queues`, refer to comment in route.go
	var queueList []string
	for _, q := range strings.Split(queues, ",") {
		if q == "" {
			continue
		}
		queueList = append(queueList, q)
	}

	ttrSecondStr := c.DefaultQuery("ttr", DefaultTTR) // Default to 1 minute
	ttrSecond, err := strconv.ParseUint(ttrSecondStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ttr"})
		return
	}

	timeoutSecondStr := c.DefaultQuery("timeout", DefaultTimeout) // Default non-blocking
	timeoutSecond, err := strconv.ParseUint(timeoutSecondStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid timeout"})
		return
	}

	// Only return a single job one request by default
	count, err := strconv.ParseUint(c.DefaultQuery("count", "1"), 10, 32)
	if count <= 0 || count > maxBatchConsumeSize || err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid count"})
		return
	}

	freezeTries, err := strconv.ParseBool(c.DefaultQuery("freeze_tries", "false"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid freeze_tries value"})
		return
	}

	if len(queueList) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid queue name(s)"})
		return
	}

	if count > 1 {
		jobs, err := e.BatchConsume(namespace, queueList, uint32(count), uint32(ttrSecond), uint32(timeoutSecond), freezeTries)
		if err != nil {
			logger.WithField("err", err).Error("Failed to batch consume")
		}
		if len(jobs) == 0 {
			c.JSON(http.StatusNotFound, gin.H{"msg": "no job available"})
			return
		}
		data := make([]map[string]interface{}, 0)
		for _, job := range jobs {
			logger.WithFields(logrus.Fields{
				"namespace": namespace,
				"queue":     job.Queue(),
				"job_id":    job.ID(),
				"ttl":       job.TTL(),
				"ttr":       ttrSecond,
			}).Debug("Job consumed")
			data = append(data, gin.H{
				"msg":          "new job",
				"namespace":    namespace,
				"queue":        job.Queue(),
				"job_id":       job.ID(),
				"data":         job.Body(), // NOTE: the body will be encoded in base64
				"ttl":          job.TTL(),
				"elapsed_ms":   job.ElapsedMS(),
				"remain_tries": job.Tries(),
			})
		}
		c.JSON(http.StatusOK, data)
		return
	}
	job, err := e.Consume(namespace, queueList, uint32(ttrSecond), uint32(timeoutSecond), freezeTries)
	if err != nil {
		logger.WithField("err", err).Error("Failed to consume")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if job == nil { // No job available
		c.JSON(http.StatusNotFound, gin.H{"msg": "no job available"})
		return
	}
	logger.WithFields(logrus.Fields{
		"namespace": namespace,
		"queue":     job.Queue(),
		"job_id":    job.ID(),
		"ttl":       job.TTL(),
		"ttr":       ttrSecond,
	}).Debug("Job consumed")
	c.JSON(http.StatusOK, gin.H{
		"msg":          "new job",
		"namespace":    namespace,
		"queue":        job.Queue(),
		"job_id":       job.ID(),
		"data":         job.Body(), // NOTE: the body will be encoded in base64
		"ttl":          job.TTL(),
		"elapsed_ms":   job.ElapsedMS(),
		"remain_tries": job.Tries(),
	})
}

// DELETE /:namespace/:queue/job/:job_id
func Delete(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")
	jobID := c.Param("job_id")

	if err := e.Delete(namespace, queue, jobID); err != nil {
		logger.WithFields(logrus.Fields{
			"err":       err,
			"namespace": namespace,
			"queue":     queue,
			"job_id":    jobID,
		}).Error("Failed to delete")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.Status(http.StatusNoContent)
}

// GET /:namespace/:queue/peek
func PeekQueue(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")

	if job, err := e.Peek(namespace, queue, ""); err != nil {
		switch err {
		case engine.ErrNotFound:
			fallthrough
		case engine.ErrEmptyQueue:
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
		logger.WithFields(logrus.Fields{
			"err":       err,
			"namespace": namespace,
			"queue":     queue,
		}).Error("Failed to peek")
		return
	} else {
		c.JSON(http.StatusOK, gin.H{
			"namespace":    namespace,
			"queue":        queue,
			"job_id":       job.ID(),
			"data":         job.Body(),
			"ttl":          job.TTL(),
			"elapsed_ms":   job.ElapsedMS(),
			"remain_tries": job.Tries(),
		})
		return
	}
}

// GET /:namespace/:queue/job/:job_id
func PeekJob(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")
	jobID := c.Param("job_id")

	if job, err := e.Peek(namespace, queue, jobID); err != nil {
		if err == engine.ErrNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		logger.WithFields(logrus.Fields{
			"err":       err,
			"namespace": namespace,
			"queue":     queue,
			"job_id":    job.ID(),
		}).Error("Failed to peek")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	} else {
		c.JSON(http.StatusOK, gin.H{
			"namespace":    namespace,
			"queue":        queue,
			"job_id":       job.ID(),
			"data":         job.Body(),
			"ttl":          job.TTL(),
			"elapsed_ms":   job.ElapsedMS(),
			"remain_tries": job.Tries(),
		})
		return
	}
}

// GET /:namespace/:queue/size
func Size(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")

	size, err := e.Size(namespace, queue)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":       err,
			"namespace": namespace,
			"queue":     queue,
		}).Error("Failed to get queue size")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"namespace": namespace,
		"queue":     queue,
		"size":      size,
	})
}

// GetDeadLetterSize return the size of dead letter
// GET /:namespace/:queue/deadletter/size
func GetDeadLetterSize(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")

	size, err := e.SizeOfDeadLetter(namespace, queue)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":       err,
			"namespace": namespace,
			"queue":     queue,
		}).Error("Failed to get queue size of dead letter")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"namespace": namespace,
		"queue":     queue,
		"size":      size,
	})
}

// GET /:namespace/:queue/deadletter
// Get the first job in the deadletter
func PeekDeadLetter(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")

	size, jobID, err := e.PeekDeadLetter(namespace, queue)
	switch err {
	case nil, engine.ErrNotFound:
		// continue
	default:
		logger.WithFields(logrus.Fields{
			"err":       err,
			"namespace": namespace,
			"queue":     queue,
		}).Error("Failed to peek deadletter")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"namespace":       namespace,
		"queue":           queue,
		"deadletter_size": size,
		"deadletter_head": jobID,
	})
}

// PUT /:namespace/:queue/deadletter
// Respawn job(s) in the deadletter
func RespawnDeadLetter(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")
	limitStr := c.DefaultQuery("limit", "1")
	limit, err := strconv.ParseInt(limitStr, 10, 64)
	if limit <= 0 || err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
		return
	}

	ttlSecondStr := c.DefaultQuery("ttl", DefaultTTL)
	ttlSecond, err := strconv.ParseInt(ttlSecondStr, 10, 64)
	if ttlSecond < 0 || err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ttl"})
		return
	}

	count, err := e.RespawnDeadLetter(namespace, queue, limit, ttlSecond)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"limit":     limitStr,
			"count":     count,
			"err":       err,
			"namespace": namespace,
			"queue":     queue,
		}).Error("Failed to delete deadletter")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	logger.WithFields(logrus.Fields{
		"namespace": namespace,
		"queue":     queue,
		"limit":     limitStr,
		"count":     count,
	}).Info("Deadletter respawned")
	c.JSON(http.StatusOK, gin.H{
		"msg":   "respawned",
		"count": count,
	})
}

// DELETE /:namespace/:queue/deadletter
// Delete job(s) in the deadletter
func DeleteDeadLetter(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")
	limitStr := c.DefaultQuery("limit", "1")

	limit, err := strconv.ParseInt(limitStr, 10, 64)
	if limit <= 0 || err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
		return
	}

	count, err := e.DeleteDeadLetter(namespace, queue, limit)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"limit":     limitStr,
			"count":     count,
			"err":       err,
			"namespace": namespace,
			"queue":     queue,
		}).Error("Failed to delete deadletter")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	logger.WithFields(logrus.Fields{
		"namespace": namespace,
		"queue":     queue,
		"limit":     limitStr,
		"count":     count,
	}).Info("Deadletter deleted")
	c.Status(http.StatusNoContent)
}

func DestroyQueue(c *gin.Context) {
	logger := GetHTTPLogger(c)
	e := c.MustGet("engine").(engine.Engine)
	namespace := c.Param("namespace")
	queue := c.Param("queue")

	count, err := e.Destroy(namespace, queue)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"count":     count,
			"err":       err,
			"namespace": namespace,
			"queue":     queue,
		}).Errorf("Failed to destroy queue")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	logger.WithFields(logrus.Fields{
		"namespace": namespace,
		"queue":     queue,
		"count":     count,
	}).Info("queue destroyed")
	c.Status(http.StatusNoContent)
}
