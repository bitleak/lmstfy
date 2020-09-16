package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/push"
)

// GET /pushers
func ListPushGroups(c *gin.Context) {
	manager := push.GetManager()
	pushers := manager.Dump()
	c.JSON(http.StatusOK, gin.H{"pushers": pushers})
}

// GET /pusher/:namespace?pool=xxx
func ListNamespacePushGroups(c *gin.Context) {
	ns := c.Param("namespace")
	pool := c.Query("pool")
	manager := push.GetManager()
	pushers := manager.ListPusherByNamespace(pool, ns)
	c.JSON(http.StatusOK, gin.H{"pushers": pushers})
}

// GET /pusher/:namespace/:group
func GetPushGroup(c *gin.Context) {
	var err error
	ns := c.Param("namespace")
	group := c.Param("group")
	pool := c.DefaultQuery("pool", config.DefaultPoolName)
	isForceRemote, _ := strconv.ParseBool(c.Param("force_remote"))
	manager := push.GetManager()
	pusher := manager.Get(pool, ns, group)
	if pusher == nil || isForceRemote {
		pusher, err = manager.GetFromRemote(pool, ns, group)
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

// POST /pusher/:namespace/:group
func CreatePushGroup(c *gin.Context) {
	var meta push.Meta
	if err := c.BindJSON(&meta); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := meta.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ns := c.Param("namespace")
	group := c.Param("group")
	pool := c.DefaultQuery("pool", config.DefaultPoolName)
	manager := push.GetManager()
	if err := manager.Create(pool, ns, group, &meta); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "created"})
}

// PUT /pusher/:namespace/:group
func UpdatePushGroup(c *gin.Context) {
	var newMeta push.Meta
	if err := c.BindJSON(&newMeta); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	manager := push.GetManager()
	ns := c.Param("namespace")
	group := c.Param("group")
	pool := c.DefaultQuery("pool", config.DefaultPoolName)
	meta, err := manager.GetFromRemote(pool, ns, group)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if meta == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "the pusher was not found"})
		return
	}
	if len(newMeta.Queues) > 0 {
		meta.Queues = newMeta.Queues
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
	if err := manager.Update(pool, ns, group, meta); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// DELETE /pusher/:namespace/:group
func DeletePushGroup(c *gin.Context) {
	manager := push.GetManager()
	ns := c.Param("namespace")
	group := c.Param("group")
	pool := c.DefaultQuery("pool", config.DefaultPoolName)
	if err := manager.Delete(pool, ns, group); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}
