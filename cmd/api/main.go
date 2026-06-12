package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/namburisnehitha/cluster-pulse/internal/cache"
	"github.com/namburisnehitha/cluster-pulse/internal/config"
	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	"github.com/namburisnehitha/cluster-pulse/internal/store"
)

func main() {

	cfg, err := config.Load("config.yaml")
	if err != nil {
		panic(err)
	}

	k8sClient, err := k8.NewClient()
	if err != nil {
		panic(err)
	}

	var s store.Store
	s, err = store.New(cfg.MySQLDSN)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	redisCache, err := cache.NewRedis(context.Background(), cfg.RedisAddr, cfg.RedisPassword)
	if err != nil {
		panic(err)
	}
	defer redisCache.Close()

	router := setupRouter(cfg, k8sClient, s, redisCache)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
