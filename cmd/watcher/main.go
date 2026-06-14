package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/namburisnehitha/cluster-pulse/internal/config"
	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	"github.com/namburisnehitha/cluster-pulse/internal/kafka"
	"github.com/namburisnehitha/cluster-pulse/internal/telemetry"
)

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down")
		cancel()
	}()

	k8sClient, err := k8.NewClient()
	if err != nil {
		log.Fatal(err)
	}

	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopicName)

	defer producer.Close()

	shutdown, err := telemetry.InitTracer("cluster-pulse-watcher")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown()

	// cold start sync
	pods, err := k8sClient.ListAllPods(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, pod := range pods {
		if !k8.IsUnhealthy(pod) {
			continue
		}
		if err := producer.Publish(ctx, kafka.PodEvent{Pod: pod, Timestamp: time.Now()}); err != nil {
			log.Println("publish error:", err)
		}
	}

	// watch loop
	podCh, err := k8sClient.WatchPods(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for result := range podCh {
		if result.Err != nil {
			log.Println("watch error:", result.Err)
			continue
		}
		if err := producer.Publish(ctx, kafka.PodEvent{Pod: result.Pod, Timestamp: time.Now()}); err != nil {
			log.Println("publish error:", err)
		}
	}

	log.Println("watcher stopped")
}
