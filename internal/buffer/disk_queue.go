package buffer

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
	"google.golang.org/protobuf/proto"
)

const (
	megaByte           = 1024 * 1024
	dqDataFile         = "overflow.bin"
	dqOffsetFile       = "overflow.offset"
	dqCompactThreshold = 64 * megaByte
)

// diskQueue is a persistent FIFO queue backed by a binary append file.
// Format per entry: [4B uint32 length][payload bytes (proto-encoded)]
// The read offset is persisted to disk so the queue survives process restarts.
type diskQueue struct {
	mu          sync.Mutex
	f           *os.File
	writeOffset int64
	readOffset  int64
	count       atomic.Int64
	dir         string
}

func newDiskQueue(dir string) (*diskQueue, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create overflow dir: %w", err)
	}

	fPath := filepath.Join(dir, dqDataFile)
	f, err := os.OpenFile(fPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open overflow file: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat overflow file: %w", err)
	}
	writeOffset := info.Size()
	readOffset := dqLoadOffset(filepath.Join(dir, dqOffsetFile))
	if readOffset > writeOffset {
		readOffset = writeOffset
	}

	q := &diskQueue{
		f:           f,
		writeOffset: writeOffset,
		readOffset:  readOffset,
		dir:         dir,
	}
	q.count.Store(int64(q.scanCount()))
	return q, nil
}

func (q *diskQueue) scanCount() int {
	pos := q.readOffset
	count := 0
	for pos < q.writeOffset {
		var lenBuf [4]byte
		n, err := q.f.ReadAt(lenBuf[:], pos)
		if err != nil || n < 4 {
			break
		}
		entryLen := int64(binary.BigEndian.Uint32(lenBuf[:]))
		pos += 4 + entryLen
		count++
	}
	return count
}

func (q *diskQueue) Enqueue(event *pb.IngestRequest) error {
	data, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(data)))
	copy(frame[4:], data)

	q.mu.Lock()
	defer q.mu.Unlock()

	if _, err := q.f.WriteAt(frame, q.writeOffset); err != nil {
		return fmt.Errorf("write overflow entry: %w", err)
	}
	if err := q.f.Sync(); err != nil {
		return fmt.Errorf("sync overflow file: %w", err)
	}
	q.writeOffset += int64(len(frame))
	q.count.Add(1)
	return nil
}

func (q *diskQueue) Dequeue() (*pb.IngestRequest, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.readOffset >= q.writeOffset {
		return nil, nil
	}

	var lenBuf [4]byte
	if n, err := q.f.ReadAt(lenBuf[:], q.readOffset); err != nil || n < 4 {
		return nil, fmt.Errorf("read entry length: %w", err)
	}
	entryLen := int64(binary.BigEndian.Uint32(lenBuf[:]))

	data := make([]byte, entryLen)
	if n, err := q.f.ReadAt(data, q.readOffset+4); err != nil || int64(n) < entryLen {
		return nil, fmt.Errorf("read entry data: %w", err)
	}

	var event pb.IngestRequest
	if err := proto.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w", err)
	}

	q.readOffset += 4 + entryLen
	q.count.Add(-1)
	q.persistOffset()

	if q.readOffset >= q.writeOffset {
		q.compact()
	} else if q.readOffset > dqCompactThreshold {
		q.compact()
	}

	return &event, nil
}

func (q *diskQueue) IsEmpty() bool {
	return q.count.Load() <= 0
}

func (q *diskQueue) Len() int {
	n := q.count.Load()
	if n < 0 {
		return 0
	}
	return int(n)
}

func (q *diskQueue) compact() {
	remaining := q.writeOffset - q.readOffset
	if remaining <= 0 {
		if err := q.f.Truncate(0); err == nil {
			q.readOffset = 0
			q.writeOffset = 0
			q.persistOffset()
		}
		return
	}

	buf := make([]byte, remaining)
	if _, err := q.f.ReadAt(buf, q.readOffset); err != nil {
		return
	}
	if _, err := q.f.WriteAt(buf, 0); err != nil {
		return
	}
	if err := q.f.Truncate(remaining); err != nil {
		return
	}
	q.readOffset = 0
	q.writeOffset = remaining
	q.persistOffset()
}

func (q *diskQueue) persistOffset() {
	path := filepath.Join(q.dir, dqOffsetFile)
	_ = os.WriteFile(path, []byte(strconv.FormatInt(q.readOffset, 10)), 0o644)
}

func (q *diskQueue) Close() error {
	return q.f.Close()
}

func dqLoadOffset(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return n
}
