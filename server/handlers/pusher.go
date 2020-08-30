package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/bitleak/lmstfy/push"
)

// GET /pushers
func ListPushers(c *gin.Context) {
	manager := push.GetManager()
	pushers := manager.Dump()
	c.JSON(http.StatusOK, gin.H{"pushers": pushers})
}

// GET /pusher/:namespace
func ListNamespacePushers(c *gin.Context) {
	ns := c.Param("namespace")
	manager := push.GetManager()
	pushers := manager.ListPusherByNamespace(ns)
	c.JSON(http.StatusOK, gin.H{"pushers": pushers})
}

// GET /pusher/:namespace/:queue
func GetQueuePusher(c *gin.Context) {
	var err error
	ns := c.Param("namespace")
	queue := c.Param("queue")
	isForceRemote, _ := strconv.ParseBool(c.Param("force_remote"))
	manager := push.GetManager()
	pusher := manager.Get(ns, queue)
	if pusher == nil || isForceRemote {
		pusher, err = manager.GetFromRemote(ns, queue)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	if pusher == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pusher was not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"pusher": pusher})
}

// POST /pusher/:namespace/:queue
func CreateQueuePusher(c *gin.Context) {
	var meta push.Meta
	if err := c.Bind(&meta); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := meta.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ns := c.Param("namespace")
	queue := c.Param("queue")
	manager := push.GetManager()
	if err := manager.Create(ns, queue, &meta); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

// PUT /pusher/:namespace/:queue
func UpdateQueuePusher(c *gin.Context) {
	var newMeta push.Meta
	if err := c.Bind(&newMeta); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	manager := push.GetManager()
	ns := c.Param("namespace")
	queue := c.Param("queue")
	meta, err := manager.GetFromRemote(ns, queue)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if meta == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "the pusher was not found"})
		return
	}
	if newMeta.Endpoint != "" {
		meta.Endpoint = newMeta.Endpoint
	}
	if newMeta.Timeout > 0 {
		meta.Timeout = newMeta.Timeout
	}
	if newMeta.Workers > 0 {
		meta.Workers = newMeta.Workers
	}
	if err := meta.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := manager.Update(ns, queue, meta); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// DELETE /pusher/:namespace/:queue
func DeleteQueuePusher(c *gin.Context) {
	manager := push.GetManager()
	ns := c.Param("namespace")
	queue := c.Param("queue")
	if err := manager.Delete(ns, queue); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}
