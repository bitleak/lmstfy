package middleware

import (
	"time"

	"github.com/bitleak/lmstfy/uuid"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

var isAccessLogEnabled = false

// IsAccessLogEnabled return whether the accesslog was enabled or not
func IsAccessLogEnabled() bool {
	return isAccessLogEnabled
}

// isAccessLogEnabled enable the accesslog
func EnableAccessLog() {
	isAccessLogEnabled = true
}

// DisableAccessLog disable the accesslog
func DisableAccessLog() {
	isAccessLogEnabled = false
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

		// Shutdown timer
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

		if !isAccessLogEnabled {
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
