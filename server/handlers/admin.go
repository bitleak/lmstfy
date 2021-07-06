package handlers

import (
	"math"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/bitleak/lmstfy/auth"
	"github.com/bitleak/lmstfy/engine"
	redis_v2 "github.com/bitleak/lmstfy/engine/redis-v2"
	"github.com/bitleak/lmstfy/server/middleware"
	"github.com/bitleak/lmstfy/throttler"
	"github.com/bitleak/lmstfy/uuid"
	"github.com/bitleak/lmstfy/version"
)

// GET /metrics
func PrometheusMetrics(c *gin.Context) {
	promhttp.Handler().ServeHTTP(c.Writer, c.Request)
}

// GET /pools/?kind=
func ListPools(c *gin.Context) {
	kind := c.DefaultQuery("kind", engine.KindRedis)
	if err := engine.ValidateKind(kind); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.IndentedJSON(http.StatusOK, engine.GetPoolsByKind(kind))
}

// GET /token/:namespace
func ListTokens(c *gin.Context) {
	tm := auth.GetTokenManager()
	tokens, err := tm.List(c.Query("pool"), c.Param("namespace"))
	if err != nil {
		logger := GetHTTPLogger(c)
		logger.WithFields(logrus.Fields{
			"pool":      c.Query("pool"),
			"namespace": c.Param("namespace"),
			"err":       err,
		}).Error("Failed to list the token")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.IndentedJSON(http.StatusOK, gin.H{"tokens": tokens})
}

// POST /token/:namespace
func NewToken(c *gin.Context) {
	c.Request.ParseForm()
	desc := c.Request.Form.Get("description") // get description from either URL query or POST form
	userToken := c.Request.Form.Get("token")  // get token from either URL query or POST form
	namespace := c.Param("namespace")
	if desc == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid description"})
		return
	}
	if userToken != "" && len(userToken) < 20 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "the user token was too short"})
		return
	}
	if len(namespace) > math.MaxUint8 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "the namespace name was too long"})
		return
	}
	if !paramRegex.MatchString(namespace) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "namespace name contains forbidden characters"})
		return
	}
	pool := c.Query("pool")
	var rawToken string
	if userToken == "" {
		rawToken = uuid.GenUniqueID()
	} else {
		fields := strings.Split(userToken, ":")
		if len(fields) == 2 {
			pool = fields[0]
			rawToken = fields[1]
		} else {
			rawToken = userToken
		}
	}

	tm := auth.GetTokenManager()
	token, err := tm.New(pool, namespace, rawToken, desc)
	if err != nil {
		if err == engine.ErrPoolNotExist || err == auth.ErrTokenExist {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		} else {
			logger := GetHTTPLogger(c)
			logger.WithFields(logrus.Fields{
				"pool":      pool,
				"namespace": namespace,
				"err":       err,
			}).Error("Failed to create the new token")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
		return
	}
	c.IndentedJSON(http.StatusCreated, gin.H{"token": token})
}

// DELETE /token/:namespace/:token
func DeleteToken(c *gin.Context) {
	logger := GetHTTPLogger(c)
	tm := auth.GetTokenManager()
	pool, token := parseToken(c.Param("token"))
	namespace := c.Param("namespace")
	if err := tm.Delete(pool, namespace, token); err != nil {
		if err == engine.ErrPoolNotExist {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		} else {
			logger.WithFields(logrus.Fields{
				"pool":      pool,
				"namespace": namespace,
				"token":     token,
				"err":       err,
			}).Error("Failed to delete the token")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
		return
	}
	if err := throttler.GetThrottler().Delete(pool, namespace, token); err != nil {
		logger.WithFields(logrus.Fields{
			"pool":      pool,
			"namespace": namespace,
			"token":     token,
			"err":       err,
		}).Error("Failed to delete the token's limiter")
	}
	c.Status(http.StatusNoContent)
}

// GET /limiters
func ListLimiters(c *gin.Context) {
	forceUpdate, _ := strconv.ParseBool(c.Query("force_update"))
	c.JSON(http.StatusOK, throttler.GetThrottler().GetAll(forceUpdate))
}

// POST /token/:namespace/:token/limit
func AddLimiter(c *gin.Context) {
	logger := GetHTTPLogger(c)
	var limiter throttler.Limiter
	if err := c.BindJSON(&limiter); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}
	if limiter.Read == 0 && limiter.Write == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"limiter": limiter})
		return
	}
	if limiter.Interval == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "interval should be > 0"})
		return
	}
	pool, token := parseToken(c.Param("token"))
	namespace := c.Param("namespace")
	err := throttler.GetThrottler().Add(pool, namespace, token, &limiter)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"token":   c.Param("token"),
			"limiter": limiter,
			"err":     err,
		}).Error("Failed to add the token's limiter")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"limiter": limiter})
}

// DELETE /token/:namespace/:token/limit
func DeleteLimiter(c *gin.Context) {
	logger := GetHTTPLogger(c)
	pool, token := parseToken(c.Param("token"))
	namespace := c.Param("namespace")
	err := throttler.GetThrottler().Delete(pool, namespace, token)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"token": c.Param("token"),
			"err":   err,
		}).Error("Failed to delete the token's limiter")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"msg": "success"})
}

// GET /token/:namespace/:token/limit
func GetLimiter(c *gin.Context) {
	pool, token := parseToken(c.Param("token"))
	namespace := c.Param("namespace")
	limiter := throttler.GetThrottler().Get(pool, namespace, token)
	if limiter == nil {
		c.JSON(http.StatusNotFound, nil)
	} else {
		c.JSON(http.StatusOK, limiter)
	}
}

// PUT /token/:namespace/:token/limit
func SetLimiter(c *gin.Context) {
	logger := GetHTTPLogger(c)
	var limiter throttler.Limiter
	if err := c.BindJSON(&limiter); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}
	if limiter.Read == 0 && limiter.Write == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"limiter": limiter})
		return
	}
	if limiter.Interval == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "interval should be > 0"})
		return
	}

	pool, token := parseToken(c.Param("token"))
	namespace := c.Param("namespace")
	err := throttler.GetThrottler().Set(pool, namespace, token, &limiter)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"token":   c.Param("token"),
			"limiter": limiter,
			"err":     err,
		}).Error("Failed to set the token's limiter")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"limiter": limiter})
}

// POST /queue/:namespace/:queue?pool=
func RegisterQueue(c *gin.Context) {
	logger := GetHTTPLogger(c)
	pool := c.Query("pool")
	namespace := c.Param("namespace")
	queue := c.Param("queue")
	e := engine.GetEngineByKind(engine.KindRedisV2, pool)
	if e == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": engine.ErrPoolNotExist.Error()})
		return
	}
	err := e.(*redis_v2.Engine).RegisterQueue(namespace, queue)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"pool":      pool,
			"namespace": namespace,
			"queue":     queue,
			"err":       err,
		}).Error("Failed to register queue")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"namespace": namespace, "queue": queue})
}

// GET /version
func Version(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, gin.H{
		"version":      version.Version,
		"build_commit": version.BuildCommit,
		"build_date":   version.BuildDate,
	})
}

func PProf(c *gin.Context) {
	switch c.Param("profile") {
	case "/profile":
		pprof.Profile(c.Writer, c.Request)
	case "/trace":
		pprof.Trace(c.Writer, c.Request)
	default:
		pprof.Index(c.Writer, c.Request)
	}
}

// GET /info?pool=&kind=
// List all namespaces and queues
func EngineMetaInfo(c *gin.Context) {
	logger := GetHTTPLogger(c)
	kind := c.DefaultQuery("kind", engine.KindRedis)
	if err := engine.ValidateKind(kind); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	e := engine.GetEngineByKind(kind, c.Query("pool"))
	if e == nil {
		return
	}
	err := e.DumpInfo(c.Writer)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"error": err,
			"kind":  kind,
			"pool":  c.Query("pool"),
		}).Error("dump engine meta info error")
		return
	}
}

// GetAccessLogStatus return whether the accesslog was enabled or not
// GET /accesslog
func GetAccessLogStatus(c *gin.Context) {
	if middleware.IsAccessLogEnabled() {
		c.JSON(http.StatusOK, gin.H{"status": "enabled"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "disabled"})
	return
}

// UpdateAccessLogStatus update the accesslog status
// POST /accesslog
func UpdateAccessLogStatus(c *gin.Context) {
	status := c.Query("status")
	if status == "enable" {
		middleware.EnableAccessLog()
		c.JSON(http.StatusOK, gin.H{"status": "enabled"})
		return
	} else if status == "disable" {
		middleware.DisableAccessLog()
		c.JSON(http.StatusOK, gin.H{"status": "disabled"})
		return
	}
	c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status"})
	return
}
