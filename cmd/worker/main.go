package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/namburisnehitha/cluster-pulse/internal/ai"
	"github.com/namburisnehitha/cluster-pulse/internal/cache"
	"github.com/namburisnehitha/cluster-pulse/internal/config"
	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	"github.com/namburisnehitha/cluster-pulse/internal/kafka"
	"github.com/namburisnehitha/cluster-pulse/internal/notifier"
	"github.com/namburisnehitha/cluster-pulse/internal/store"
)

const (
	groqBaseURL = "https://api.groq.com/openai/v1"
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

	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopicName, "cluster-pulse-worker")
	defer consumer.Close()

	redisCache, err := cache.NewRedis(ctx, cfg.RedisAddr, cfg.RedisPassword)
	if err != nil {
		log.Fatal(err)
	}
	defer redisCache.Close()

	mysqlStore, err := store.New(cfg.MySQLDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer mysqlStore.Close()

	k8sClient, err := k8.NewClient()
	if err != nil {
		log.Fatal(err)
	}

	primaryAnalyzer := ai.NewOpenAIAnalyzer(cfg.GroqAPIKey, groqBaseURL, cfg.GroqModel)
	fallbackAnalyzer := ai.NewOpenAIAnalyzer(cfg.OpenAIAPIKey, "", cfg.OpenAIModel)

	slackNotifier := notifier.NewNotifier(cfg.SlackWebhookURL)

	sem := make(chan struct{}, cfg.WorkerConcurrencyLimit)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			log.Println("worker stopped")
			return
		default:
		}

		event, err := consumer.Consume(ctx)
		if err != nil {
			log.Println("consume error:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		key := "analyzed:" + event.Pod.Namespace + "/" + event.Pod.Name
		exists, err := redisCache.Exists(ctx, key)
		if err != nil {
			log.Println("cache check error:", err)
		} else if exists {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(event kafka.PodEvent, key string) {
			defer wg.Done()
			defer func() { <-sem }()

			history, err := mysqlStore.GetPodHistory(ctx, event.Pod.Name, event.Pod.Namespace, 10)
			if err != nil {
				log.Println("get history error:", err)
			}
			trend := store.ComputeTrend(history)

			analysis, err := primaryAnalyzer.Analyze(ctx, event, trend)
			if err != nil {
				log.Println("primary analyzer error:", err)
				analysis, err = fallbackAnalyzer.Analyze(ctx, event, trend)
				if err != nil {
					log.Println("fallback analyzer error:", err)
					return
				}
			}

			analysis.PodName = event.Pod.Name
			analysis.Namespace = event.Pod.Namespace
			analysis.FailureTime = event.Pod.FailureTime

			if err := mysqlStore.SaveAnalysis(ctx, analysis); err != nil {
				log.Println("save analysis error:", err)
			}

			cpuUsage, memUsage, err := k8sClient.GetPodMetrics(ctx, event.Pod.Namespace, event.Pod.Name)
			if err != nil {
				log.Println("get pod metrics error:", err)
			} else {
				snap := store.ResourceSnapshot{
					PodName:     event.Pod.Name,
					Namespace:   event.Pod.Namespace,
					CPUUsage:    cpuUsage,
					MemoryUsage: memUsage,
					RecordedAt:  time.Now(),
				}
				if err := mysqlStore.SaveResourceSnapshot(ctx, snap); err != nil {
					log.Println("save snapshot error:", err)
				}
			}

			if err := redisCache.Set(ctx, key, "1", 5*time.Minute); err != nil {
				log.Println("cache set error:", err)
			}

			if err := slackNotifier.Notify(ctx, analysis); err != nil {
				log.Println("notify error:", err)
			}
		}(event, key)
	}
}
