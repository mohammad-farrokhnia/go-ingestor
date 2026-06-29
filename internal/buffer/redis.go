package buffer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
	"github.com/redis/go-redis/v9"
)

type RedisBuffer struct {
	client *redis.Client
	key    string
	ch     chan *pb.IngestRequest
	ctx    context.Context
	cancel context.CancelFunc
	pollWg sync.WaitGroup
}

func NewRedisBuffer(cfg config.RedisBufferConfig, chanSize int) (*RedisBuffer, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	key := cfg.Key
	if key == "" {
		key = "ingestor:buffer"
	}

	pollCtx, cancel := context.WithCancel(context.Background())

	rb := &RedisBuffer{
		client: client,
		key:    key,
		ch:     make(chan *pb.IngestRequest, chanSize),
		ctx:    pollCtx,
		cancel: cancel,
	}

	rb.pollWg.Add(1)
	go rb.poll()

	slog.Info("Redis buffer initialized", "addr", cfg.Addr, "key", key)
	return rb, nil
}

func (b *RedisBuffer) Push(event *pb.IngestRequest) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	return b.client.LPush(b.ctx, b.key, data).Err()
}

func (b *RedisBuffer) Chan() <-chan *pb.IngestRequest {
	return b.ch
}

func (b *RedisBuffer) Close() error {
	// Signal the poll goroutine to stop and wait for it to finish *before*
	// closing the channel. Otherwise poll could reach `b.ch <- &event` after
	// the channel is closed and panic on send to a closed channel.
	b.cancel()
	b.pollWg.Wait()
	close(b.ch)
	return b.client.Close()
}

func (b *RedisBuffer) Len() int {
	n, err := b.client.LLen(b.ctx, b.key).Result()
	if err != nil {
		return 0
	}
	return int(n)
}

func (b *RedisBuffer) poll() {
	defer b.pollWg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		result, err := b.client.BRPop(b.ctx, 1*time.Second, b.key).Result()
		if err != nil {
			if err == redis.Nil || b.ctx.Err() != nil {
				continue
			}
			slog.Error("Redis BRPOP error", "err", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if len(result) < 2 {
			continue
		}

		var event pb.IngestRequest
		if err := json.Unmarshal([]byte(result[1]), &event); err != nil {
			slog.Error("Failed to unmarshal event from Redis", "err", err)
			continue
		}

		select {
		case b.ch <- &event:
		case <-b.ctx.Done():
			return
		}
	}
}
