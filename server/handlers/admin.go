package handlers

import (
	"math"
	"net/http"
	"net/http/pprof"

	"github.com/gin-gonic/gin"
	"github.com/meitu/lmstfy/auth"
	"github.com/meitu/lmstfy/engine"
	"github.com/meitu/lmstfy/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// GET /metrics
func PrometheusMetrics(c *gin.Context) {
	promhttp.Handler().ServeHTTP(c.Writer, c.Request)
}

// GET /token/:namespace
func ListTokens(c *gin.Context) {
	tm := auth.GetTokenManager()
	tokens, err := tm.List(c.Query("pool"), c.Param("namespace"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.IndentedJSON(http.StatusOK, gin.H{"tokens": tokens})
}

// POST /token/:namespace
func NewToken(c *gin.Context) {
	c.Request.ParseForm()
	desc := c.Request.Form.Get("description") // get description from either URL query or POST form
	if desc == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid description"})
		return
	}
	if len(c.Param("namespace")) > math.MaxUint8 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "namespace name too long"})
		return
	}
	tm := auth.GetTokenManager()
	token, err := tm.New(c.Query("pool"), c.Param("namespace"), desc)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.IndentedJSON(http.StatusCreated, gin.H{"token": token})
}

// DELETE /token/:namespace/:token
func DeleteToken(c *gin.Context) {
	tm := auth.GetTokenManager()
	if err := tm.Delete(c.Query("pool"), c.Param("namespace"), c.Param("token")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
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
