package handlers

import (
	"math"
	"net/http"
	"regexp"
	"strings"

	"github.com/bitleak/lmstfy/auth"
	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/engine"
	"github.com/gin-gonic/gin"
)

func getToken(c *gin.Context) (token string) {
	token = c.GetHeader("X-Token")
	if token == "" {
		token = c.Query("token")
	}
	return
}

// The user token's format is: [{pool}:]{token}
// there is a optional pool prefix, if provided, use that pool; otherwise use the default pool.
func parseToken(rawToken string) (pool, token string) {
	splits := strings.SplitN(rawToken, ":", 2)
	if len(splits) == 2 {
		return splits[0], splits[1]
	}
	return config.DefaultPoolName, rawToken
}

func SetupEngine(c *gin.Context) {
	pool, token := parseToken(getToken(c))
	c.Set("pool", pool)
	c.Set("token", token)
	e := engine.GetEngine(pool)
	if e == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pool not found"})
		c.Abort()
		return
	}
	c.Set("engine", e)
}

func SetupQueue(c *gin.Context) {
	e := c.MustGet("engine").(engine.Engine)
	c.Set("queue", e.Queue(engine.QueueMeta{
		Namespace: c.Param("namespace"),
		Queue:     c.Param("queue"),
	}))
}

func SetupQueues(c *gin.Context) {
	e := c.MustGet("engine").(engine.Engine)
	queues := c.Param("queue") // NOTE: param name should be `queues`, refer to comment in route.go
	namespace := c.Param("namespace")
	var metaList []engine.QueueMeta
	for _, q := range strings.Split(queues, ",") {
		if q == "" {
			continue
		}
		metaList = append(metaList, engine.QueueMeta{
			Namespace: namespace,
			Queue:     q,
		})
	}
	if len(metaList) == 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid queue name(s)"})
		return
	}
	c.Set("queue", e.Queues(metaList))
}

func SetupDeadLetter(c *gin.Context) {
	e := c.MustGet("engine").(engine.Engine)
	c.Set("deadletter", e.DeadLetter(engine.QueueMeta{
		Namespace: c.Param("namespace"),
		Queue:     c.Param("queue"),
	}))
}

func ValidateToken(c *gin.Context) {
	tk := c.GetString("token")
	if tk == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "token not found"})
		c.Abort()
		return
	}
	tm := auth.GetTokenManager()
	ok, err := tm.Exist(c.GetString("pool"), c.Param("namespace"), tk)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		c.Abort()
		return
	}
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
		c.Abort()
		return
	}
}

var paramRegex = regexp.MustCompile("^[-_[:alnum:]]+$")
var multiQueuesRegex = regexp.MustCompile("^[-_,[:alnum:]]+$")

// Validate namespace and queue names don't contain any illegal characters
func ValidateParams(c *gin.Context) {
	ns := c.Param("namespace")
	q := c.Param("queue")
	if len(ns) > math.MaxUint8 || len(q) > math.MaxUint8 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "namespace or queue name too long"})
		c.Abort()
		return
	}
	if !paramRegex.MatchString(ns) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "namespace name contains forbidden characters"})
		c.Abort()
		return
	}
	if strings.HasPrefix(q, "_") || !paramRegex.MatchString(q) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "queue name contains forbidden characters"})
		c.Abort()
		return
	}
}

func ValidateMultiConsume(c *gin.Context) {
	ns := c.Param("namespace")
	q := c.Param("queue")
	if !paramRegex.MatchString(ns) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "namespace name contains forbidden characters"})
		c.Abort()
		return
	}
	if !multiQueuesRegex.MatchString(q) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "queue name contains forbidden characters"})
		c.Abort()
		return
	}
}

func CheckPoolExists(c *gin.Context) {
	pool := c.Query("pool")
	if exists := engine.ExistsPool(pool); !exists {
		c.JSON(http.StatusBadRequest, auth.ErrPoolNotExist)
		c.Abort()
	}
}
