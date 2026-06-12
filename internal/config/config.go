package config

import (
	"os"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	MySQLDSN                      string   `mapstructure:"mysql_dsn"`
	RedisAddr                     string   `mapstructure:"redis_addr"`
	RedisPassword                 string   `mapstructure:"redis_password"`
	SlackWebhookURL               string   `mapstructure:"slack_webhook_url"`
	KafkaBrokers                  []string `mapstructure:"kafka_brokers"`
	KafkaTopicName                string   `mapstructure:"kafka_topic_name"`
	GroqAPIKey                    string   `mapstructure:"groq_api_key"`
	OpenAIAPIKey                  string   `mapstructure:"openai_api_key"`
	GroqModel                     string   `mapstructure:"groq_model"`
	OpenAIModel                   string   `mapstructure:"openai_model"`
	JWTSecret                     string   `mapstructure:"jwt_secret"`
	AdminUsername                 string   `mapstructure:"admin_username"`
	AdminPasswordHash             string   `mapstructure:"admin_password_hash"`
	WorkerConcurrencyLimit        int      `mapstructure:"worker_concurrency_limit"`
	ResourceSnapshotRetentionDays int      `mapstructure:"resource_snapshot_retention_days"`
}

func Load(path string) (*Config, error) {
	viper.SetConfigFile(path)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config

	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.KafkaBrokers = strings.Split(brokers, ",")
	}

	if dsn := os.Getenv("MYSQLDSN"); dsn != "" {
		cfg.MySQLDSN = dsn
	}

	if addr := os.Getenv("REDISADDR"); addr != "" {
		cfg.RedisAddr = addr
	}

	return &cfg, nil
}
