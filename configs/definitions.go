package config

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

type SinkConfig struct {
	Active bool `mapstructure:"active"`
}
