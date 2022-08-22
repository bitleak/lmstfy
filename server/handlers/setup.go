package handlers

import (
	"strconv"
	"sync"

	"github.com/bitleak/lmstfy/config"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

var setupOnce sync.Once
var _logger *logrus.Logger

var (
	DefaultTTL     string
	DefaultDelay   string
	DefaultTries   string
	DefaultTTR     string
	DefaultTimeout string
)

func Setup(l *logrus.Logger) {
	setupOnce.Do(func() {
		_logger = l
		setupMetrics()
	})
}

func GetHTTPLogger(c *gin.Context) *logrus.Entry {
	reqID := c.GetString("req_id")
	if reqID == "" {
		return logrus.NewEntry(_logger)
	}
	return _logger.WithField("req_id", reqID)
}

func SetupParamDefaults(conf *config.Config) {
	DefaultTTL = strconv.Itoa(conf.TTLSecond)
	DefaultDelay = strconv.Itoa(conf.DelaySecond)
	DefaultTries = strconv.Itoa(conf.TriesNum)
	DefaultTTR = strconv.Itoa(conf.TTRSecond)
	DefaultTimeout = strconv.Itoa(conf.TimeoutSecond)
}
