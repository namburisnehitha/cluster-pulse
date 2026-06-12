package main

import (
	"github.com/gin-gonic/gin"

	"github.com/namburisnehitha/cluster-pulse/internal/cache"
	"github.com/namburisnehitha/cluster-pulse/internal/config"
	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	"github.com/namburisnehitha/cluster-pulse/internal/store"
)

func setupRouter(cfg *config.Config, k8sClient *k8.Client, s store.Store, redisCache *cache.Redis) *gin.Engine {
	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	router.POST("/login", loginHandler(cfg))

	authorized := router.Group("/")
	authorized.Use(authMiddleware(cfg.JWTSecret))
	{
		authorized.GET("/cluster/pods", clusterPodsHandler(k8sClient))
		authorized.GET("/cluster/unhealthy", clusterUnhealthyHandler(k8sClient))
		authorized.GET("/analyses", listAnalysesHandler(s))
		authorized.GET("/analysis/:pod", getAnalysisHandler(s, redisCache))
		authorized.GET("/metrics", podMetricsHandler(k8sClient))
		authorized.GET("/analysis/:pod/history", podHistoryHandler(s))
		authorized.GET("/cluster/nodes", clusterNodesHandler(k8sClient))
		authorized.GET("/cluster/events", clusterEventsHandler(k8sClient))
	}

	return router
}
