package main

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"net/http"

	"github.com/bitleak/lmstfy/server/handlers"
)

func SetupRoutes(e *gin.Engine, logger *logrus.Logger, devMode bool) {
	handlers.Setup(logger)
	group := e.Group("/api")
	group.Use(handlers.ValidateParams, handlers.SetupEngine, handlers.SetupQueue)
	if !devMode {
		group.Use(handlers.ValidateToken)
	}
	group.PUT("/:namespace/:queue", handlers.Throttle(handlers.ThrottleActionProduce), handlers.Publish)
	group.PUT("/:namespace/:queue/bulk", handlers.Throttle(handlers.ThrottleActionProduce), handlers.PublishBulk)
	group.PUT("/:namespace/:queue/job/:job_id", handlers.Publish)
	group.GET("/:namespace/:queue/peek", handlers.PeekQueue)
	group.GET("/:namespace/:queue/job/:job_id", handlers.PeekJob)
	group.DELETE("/:namespace/:queue/job/:job_id", handlers.Delete)
	group.DELETE("/:namespace/:queue", handlers.DestroyQueue)
	// Consume API is special, it accepts the url like `/api/namespace/q1,q2,q3`,
	// while all other APIs forbid.
	group2 := e.Group("/api")
	group2.Use(handlers.ValidateMultiConsume, handlers.SetupEngine, handlers.SetupQueues)
	if !devMode {
		group2.Use(handlers.ValidateToken)
	}
	// NOTE: the route should be named /:namespace/:queues, but gin http-router reports conflict
	// when mixing /:queue and /:queues together, :(
	group2.GET("/:namespace/:queue", handlers.Throttle(handlers.ThrottleActionConsume), handlers.Consume)

	// Dead letter
	deadLetterGroup := e.Group("/api")
	deadLetterGroup.Use(handlers.ValidateParams, handlers.SetupEngine, handlers.SetupDeadLetter)
	if !devMode {
		deadLetterGroup.Use(handlers.ValidateToken)
	}
	deadLetterGroup.GET("/:namespace/:queue/deadletter", handlers.PeekDeadLetter)
	deadLetterGroup.PUT("/:namespace/:queue/deadletter", handlers.RespawnDeadLetter)
	deadLetterGroup.DELETE("/:namespace/:queue/deadletter", handlers.DeleteDeadLetter)

	// Public API group
	pubGroup := e.Group("/api")
	pubGroup.Use(handlers.ValidateParams, handlers.SetupEngine)
	pubGroup.GET("/:namespace/:queue/size", handlers.SetupQueue, handlers.Size)
	pubGroup.GET("/:namespace/:queue/deadletter/size", handlers.SetupDeadLetter, handlers.GetDeadLetterSize)

	e.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{"error": "api not found"})
	})
}

func SetupAdminRoutes(e *gin.Engine, accounts gin.Accounts) {
	basicAuthMiddleware := func(c *gin.Context) { c.Next() }
	if len(accounts) > 0 {
		basicAuthMiddleware = gin.BasicAuth(accounts)
	}

	e.GET("/info", handlers.EngineMetaInfo)
	e.GET("/version", handlers.Version)
	e.GET("/metrics", handlers.PrometheusMetrics)
	e.GET("/pools", handlers.ListPools)

	// token's limit URI
	e.GET("/limits", basicAuthMiddleware, handlers.ListLimiters)

	tokenGroup := e.Group("/token")
	{
		tokenGroup.Use(basicAuthMiddleware)
		tokenGroup.GET("/:namespace", handlers.ListTokens)
		tokenGroup.POST("/:namespace", handlers.NewToken)
		tokenGroup.DELETE("/:namespace/:token", handlers.DeleteToken)
		tokenGroup.GET("/:namespace/:token/limit", handlers.GetLimiter)
		tokenGroup.POST("/:namespace/:token/limit", handlers.AddLimiter)
		tokenGroup.PUT("/:namespace/:token/limit", handlers.SetLimiter)
		tokenGroup.DELETE("/:namespace/:token/limit", handlers.DeleteLimiter)
	}

	e.Any("/debug/pprof/*profile", handlers.PProf)
	e.GET("/accesslog", handlers.GetAccessLogStatus)
	e.POST("/accesslog", basicAuthMiddleware, handlers.UpdateAccessLogStatus)
}
