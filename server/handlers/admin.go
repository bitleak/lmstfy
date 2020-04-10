package handlers

import (
	"math"
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/bitleak/lmstfy/auth"
	"github.com/bitleak/lmstfy/engine"
	"github.com/bitleak/lmstfy/server/middleware"
	"github.com/bitleak/lmstfy/uuid"
	"github.com/bitleak/lmstfy/version"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

// GET /metrics
func PrometheusMetrics(c *gin.Context) {
	promhttp.Handler().ServeHTTP(c.Writer, c.Request)
}

// GET /pools/
func ListPools(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, engine.GetPools())
}

// GET /token/:namespace
func ListTokens(c *gin.Context) {
	tm := auth.GetTokenManager()
	tokens, err := tm.List(c.Query("pool"), c.Param("namespace"))
	if err != nil {
		if err == auth.ErrPoolNotExist {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		} else {
			logger := GetHTTPLogger(c)
			logger.WithFields(logrus.Fields{
				"pool":      c.Query("pool"),
				"namespace": c.Param("namespace"),
				"err":       err,
			}).Error("Failed to list the token")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
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
		if err == auth.ErrPoolNotExist || err == auth.ErrTokenExist {
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
	tm := auth.GetTokenManager()
	if err := tm.Delete(c.Query("pool"), c.Param("namespace"), c.Param("token")); err != nil {
		if err == auth.ErrPoolNotExist {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		} else {
			logger := GetHTTPLogger(c)
			logger.WithFields(logrus.Fields{
				"pool":      c.Query("pool"),
				"namespace": c.Param("namespace"),
				"token":     c.Param("token"),
				"err":       err,
			}).Error("Failed to delete the token")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
		return
	}
	c.Status(http.StatusNoContent)
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

// GET /info?pool=
// List all namespaces and queues
func EngineMetaInfo(c *gin.Context) {
	e := engine.GetEngineByKind("redis", c.Query("pool"))
	if e == nil {
		return
	}
	e.DumpInfo(c.Writer)
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
