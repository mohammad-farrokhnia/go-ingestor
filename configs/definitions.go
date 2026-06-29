package config

type (
	BufferType string
	SinkType   string
	DLQType    string
)

const (
	SinkLog   SinkType = "log"
	SinkKafka SinkType = "kafka"
	SinkHTTP  SinkType = "http"

	ChannelBuffer BufferType = "channel"
	RedisBuffer   BufferType = "redis"
	HybridBuffer  BufferType = "hybrid"

	DLQTypeFile  DLQType = "file"
	DLQTypeKafka DLQType = "kafka"
)

const (
	DefaultHybridBufferDir = "data/buffer"

	DefaultWALDir             = "data/wal"
	DefaultCheckpointInterval = "60s"
)

type RedisBufferConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	Key      string `mapstructure:"key"`
}

type HybridBufferConfig struct {
	Dir string `mapstructure:"dir"`
}

type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
}

type HTTPConfig struct {
	URL     string `mapstructure:"url"`
	Timeout string `mapstructure:"timeout"`
}

type FileDLQConfig struct {
	Dir string `mapstructure:"dir"`
}

type KafkaDLQConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
}

type BufferConfig struct {
	Type   BufferType         `mapstructure:"type"`
	Redis  RedisBufferConfig  `mapstructure:"redis"`
	Hybrid HybridBufferConfig `mapstructure:"hybrid"`
}

type SinksConfig struct {
	Active []SinkType  `mapstructure:"active" json:"active,omitempty"`
	Kafka  KafkaConfig `mapstructure:"kafka" json:"kafka"`
	HTTP   HTTPConfig  `mapstructure:"http" json:"http"`
}

type DLQConfig struct {
	Enabled bool           `mapstructure:"enabled"`
	Type    DLQType        `mapstructure:"type"`
	File    FileDLQConfig  `mapstructure:"file"`
	Kafka   KafkaDLQConfig `mapstructure:"kafka"`
}

type ServerConfig struct {
	GrpcPort      int    `mapstructure:"grpc_port"`
	HttpPort      int    `mapstructure:"http_port"`
	IngestEnabled bool   `mapstructure:"ingest_enabled"`
	AdminToken    string `mapstructure:"admin_token"`
}

type IngestorConfig struct {
	BufferSize int `mapstructure:"buffer_size"`
}

type WorkerConfig struct {
	NumWorkers   int    `mapstructure:"num_workers"`
	BatchSize    int    `mapstructure:"batch_size"`
	BatchTimeout string `mapstructure:"batch_timeout"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type WALConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	Dir                string `mapstructure:"dir"`
	CheckpointInterval string `mapstructure:"checkpoint_interval"`
}

type ShutdownConfig struct {
	Timeout string `mapstructure:"timeout"`
}

// TenancyConfig controls multi-tenancy. When Enabled is true every ingested
// event must carry a non-empty tenant_id; when false (the default) the gateway
// behaves exactly as a single-tenant service.
type TenancyConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Ingestor IngestorConfig `mapstructure:"ingestor"`
	Buffer   BufferConfig   `mapstructure:"buffer"`
	Worker   WorkerConfig   `mapstructure:"worker"`
	Sinks    SinksConfig    `mapstructure:"sinks"`
	DLQ      DLQConfig      `mapstructure:"dlq"`
	WAL      WALConfig      `mapstructure:"wal"`
	Logging  LoggingConfig  `mapstructure:"logging"`
	Shutdown ShutdownConfig `mapstructure:"shutdown"`
	Tenancy  TenancyConfig  `mapstructure:"tenancy"`
}
