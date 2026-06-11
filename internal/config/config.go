package config

import "github.com/spf13/viper"

type Config struct {
	MySQLDSN                      string
	RedisAddr                     string
	RedisPassword                 string
	SlackWebhookURL               string
	KafkaBrokers                  []string
	KafkaTopicName                string
	GroqAPIKey                    string
	OpenAIAPIKey                  string
	GroqModel                     string
	OpenAIModel                   string
	JWTSecret                     string
	WorkerConcurrencyLimit        int
	ResourceSnapshotRetentionDays int
	AdminUsername                 string
	AdminPasswordHash             string
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

	return &cfg, nil
}
