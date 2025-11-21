package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Ingestor IngestorConfig `mapstructure:"ingestor"`
	Worker   WorkerConfig   `mapstructure:"worker"`
}

type ServerConfig struct {
	GrpcPort      int  `mapstructure:"grpc_port"`
	HttpPort      int  `mapstructure:"http_port"`
	IngestEnabled bool `mapstructure:"ingest_enabled"`
}

type IngestorConfig struct {
	BufferSize int `mapstructure:"buffer_size"`
}

type WorkerConfig struct {
	NumWorkers   int    `mapstructure:"num_workers"`
	BatchSize    int    `mapstructure:"batch_size"`
	BatchTimeout string `mapstructure:"batch_timeout"`
}

func Load(path string) (*Config, error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
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
