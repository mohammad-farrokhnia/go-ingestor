package config

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Ingestor IngestorConfig `mapstructure:"ingestor"`
	Worker   WorkerConfig   `mapstructure:"worker"`
	Sinks    SinksConfig    `mapstructure:"sinks"`
	DLQ      DLQConfig      `mapstructure:"dlq"`
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

type SinksConfig struct {
	Active []SinkType  `mapstructure:"active" json:"active,omitempty"`
	Kafka  KafkaConfig `mapstructure:"kafka" json:"kafka"`
	HTTP   HTTPConfig  `mapstructure:"http" json:"http"`
}

type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
}

type HTTPConfig struct {
	URL     string `mapstructure:"url"`
	Timeout string `mapstructure:"timeout"`
}

type SinkType string

const (
	SinkLog   SinkType = "log"
	SinkKafka SinkType = "kafka"
	SinkHTTP  SinkType = "http"
)

type DLQConfig struct {
	Enabled bool           `mapstructure:"enabled"`
	Type    string         `mapstructure:"type"`
	File    FileDLQConfig  `mapstructure:"file"`
	Kafka   KafkaDLQConfig `mapstructure:"kafka"`
}

type FileDLQConfig struct {
	Dir string `mapstructure:"dir"`
}

type KafkaDLQConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
}

type DLQType string

const (
	DLQTypeFile  DLQType = "file"
	DLQTypeKafka DLQType = "kafka"
)
