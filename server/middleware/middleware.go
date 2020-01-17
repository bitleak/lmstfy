package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/meitu/lmstfy/uuid"
	"github.com/sirupsen/logrus"
)

// enableAccessLog control whether accesslog output
var enableAccessLog = false

// AccessLogStatus return whether whether accesslog output
func AccessLogStatus() bool {
	return enableAccessLog
}

// EnableAccessLog enable accesslog output
func EnableAccessLog() {
	enableAccessLog = true
}

// DisableAccessLog disable accesslog output
func DisableAccessLog() {
	enableAccessLog = false
}

// RequestIDMiddleware set request uuid into context
func RequestIDMiddleware(c *gin.Context) {
	reqID := uuid.GenUniqueID()
	c.Set("req_id", reqID)
	c.Header("X-Request-ID", reqID)
}

// AccessLogMiddleware generate accesslog and output
func AccessLogMiddleware(logger *logrus.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Start timer
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		// Process request
		c.Next()

		// Stop timer
		end := time.Now()
		latency := end.Sub(start)

		clientIP := c.ClientIP()
		method := c.Request.Method
		statusCode := c.Writer.Status()

		fields := logrus.Fields{
			"pool":    c.GetString("pool"),
			"path":    path,
			"query":   query,
			"latency": latency,
			"ip":      clientIP,
			"method":  method,
			"code":    statusCode,
			"req_id":  c.GetString("req_id"),
		}

		if !enableAccessLog {
			return
		}

		if statusCode >= 500 {
			logger.WithFields(fields).Error()
		} else if statusCode >= 400 && statusCode != 404 {
			logger.WithFields(fields).Warn()
		} else {
			logger.WithFields(fields).Info()
		}
	}
}
