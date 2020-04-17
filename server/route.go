package main

import (
	"net/http"

	"github.com/bitleak/lmstfy/server/handlers"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func SetupRoutes(e *gin.Engine, logger *logrus.Logger, devMode bool) {
	handlers.Setup(logger)

	group := e.Group("/api")
	group.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	if !devMode {
		group.Use(handlers.ValidateToken)
	}
	group.PUT("/:namespace/:queue", handlers.CollectMetrics("publish"), handlers.Publish)
	group.PUT("/:namespace/:queue/job/:job_id", handlers.CollectMetrics("publish_delete"), handlers.Publish)
	group.GET("/:namespace/:queue/peek", handlers.PeekQueue)
	group.GET("/:namespace/:queue/job/:job_id", handlers.PeekJob)
	group.DELETE("/:namespace/:queue/job/:job_id", handlers.CollectMetrics("delete"), handlers.Delete)
	group.DELETE("/:namespace/:queue", handlers.DestroyQueue)

	// Consume API is special, it accepts the url like `/api/namespace/q1,q2,q3`,
	// while all other APIs forbid.
	group2 := e.Group("/api")
	group2.Use(handlers.ValidateMultiConsume, handlers.SetupQueueEngine)
	if !devMode {
		group2.Use(handlers.ValidateToken)
	}
	// NOTE: the route should be named /:namespace/:queues, but gin http-router reports conflict
	// when mixing /:queue and /:queues together, :(
	group2.GET("/:namespace/:queue", handlers.CollectMetrics("consume"), handlers.Consume)

	// Dead letter
	group.GET("/:namespace/:queue/deadletter", handlers.PeekDeadLetter)
	group.PUT("/:namespace/:queue/deadletter", handlers.RespawnDeadLetter)
	group.DELETE("/:namespace/:queue/deadletter", handlers.DeleteDeadLetter)

	// Public API group
	pubGroup := e.Group("/api")
	pubGroup.Use(handlers.ValidateParams, handlers.SetupQueueEngine)
	pubGroup.GET("/:namespace/:queue/size", handlers.Size)
	pubGroup.GET("/:namespace/:queue/deadletter/size", handlers.GetDeadLetterSize)

	e.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{"error": "api not found"})
	})
}
