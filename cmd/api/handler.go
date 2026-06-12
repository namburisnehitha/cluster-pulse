package main

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/namburisnehitha/cluster-pulse/internal/cache"
	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	"github.com/namburisnehitha/cluster-pulse/internal/store"
)

func clusterPodsHandler(k8sClient *k8.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		pods, err := k8sClient.ListAllPods(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, pods)
	}
}

func clusterUnhealthyHandler(k8sClient *k8.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		pods, err := k8sClient.ListAllPods(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		var unhealthy []k8.Pod
		for _, p := range pods {
			if k8.IsUnhealthy(p) {
				unhealthy = append(unhealthy, p)
			}
		}
		c.JSON(http.StatusOK, unhealthy)
	}
}

func listAnalysesHandler(s store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		cursor := c.Query("cursor")
		limitStr := c.DefaultQuery("limit", "20")

		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			limit = 20
		}

		analyses, nextCursor, err := s.ListAnalyses(c.Request.Context(), cursor, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"analyses":    analyses,
			"next_cursor": nextCursor,
		})
	}
}

func getAnalysisHandler(s store.Store, redisCache *cache.Redis) gin.HandlerFunc {
	return func(c *gin.Context) {
		podName := c.Param("pod")
		namespace := c.DefaultQuery("namespace", "default")

		key := "analyzed:" + namespace + "/" + podName
		if cached, err := redisCache.Get(c.Request.Context(), key); err == nil {
			c.Data(http.StatusOK, "application/json", []byte(cached))
			return
		}

		analysis, err := s.GetAnalysis(c.Request.Context(), podName, namespace)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if analysis == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "no analysis found for this pod"})
			return
		}

		c.JSON(http.StatusOK, analysis)
	}
}

func podMetricsHandler(k8sClient *k8.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		podName := c.Query("pod")
		namespace := c.Query("namespace")
		if podName == "" || namespace == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "pod and namespace query params required"})
			return
		}

		cpu, mem, err := k8sClient.GetPodMetrics(c.Request.Context(), namespace, podName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"cpu": cpu, "memory": mem})
	}
}

func podHistoryHandler(s store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		podName := c.Param("pod")
		namespace := c.DefaultQuery("namespace", "default")
		history, err := s.GetPodHistory(c.Request.Context(), podName, namespace, 10)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, history)
	}
}

func clusterNodesHandler(k8sClient *k8.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		nodes, err := k8sClient.ListAllNodes(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, nodes)
	}
}

func clusterEventsHandler(k8sClient *k8.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		namespace := c.DefaultQuery("namespace", "")
		events, err := k8sClient.ListAllEvents(c.Request.Context(), namespace)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, events)
	}
}
