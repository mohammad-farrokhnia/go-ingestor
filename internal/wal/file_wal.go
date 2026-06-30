package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

const (
	walFileName = "wal.log"
	ackFileName = "wal.ack"
)

// headerSize is the fixed per-record header: [8-byte seq][4-byte payload length].
const headerSize = 12

// record is one WAL entry on disk: a sequence number and its protobuf payload.
type record struct {
	seq     uint64
	payload []byte
}

// writeRecord encodes a record as [8-byte seq][4-byte len][payload].
func writeRecord(w io.Writer, rec record) error {
	var header [headerSize]byte
	binary.BigEndian.PutUint64(header[0:8], rec.seq)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(rec.payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(rec.payload); err != nil {
		return err
	}
	return nil
}

// readRecord decodes the next record. It returns io.EOF at a clean end of log
// and io.ErrUnexpectedEOF on a torn final record (a partial trailing write).
func readRecord(r io.Reader) (record, error) {
	var header [headerSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return record{}, err
	}
	seq := binary.BigEndian.Uint64(header[0:8])
	length := binary.BigEndian.Uint32(header[8:12])

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return record{}, err
	}
	return record{seq: seq, payload: payload}, nil
}

type FileWAL struct {
	mu      sync.Mutex
	dir     string
	walFile *os.File
	ackFile *os.File
	seq     uint64
	acked   map[uint64]struct{}
}

func NewFileWAL(dir string) (*FileWAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}

	walPath := filepath.Join(dir, walFileName)
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open wal file: %w", err)
	}

	ackPath := filepath.Join(dir, ackFileName)
	ackFile, err := os.OpenFile(ackPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		_ = walFile.Close()
		return nil, fmt.Errorf("wal: open ack file: %w", err)
	}

	fw := &FileWAL{
		dir:     dir,
		walFile: walFile,
		ackFile: ackFile,
		acked:   make(map[uint64]struct{}),
	}

	if err := fw.loadAcked(); err != nil {
		_ = walFile.Close()
		_ = ackFile.Close()
		return nil, fmt.Errorf("wal: load acked: %w", err)
	}

	if err := fw.loadMaxSeq(); err != nil {
		_ = walFile.Close()
		_ = ackFile.Close()
		return nil, fmt.Errorf("wal: load max seq: %w", err)
	}

	return fw, nil
}

func (fw *FileWAL) Append(event *pb.IngestRequest) (uint64, error) {
	data, err := proto.Marshal(event)
	if err != nil {
		return 0, fmt.Errorf("wal: marshal event: %w", err)
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()

	fw.seq++
	seqNum := fw.seq

	if err := writeRecord(fw.walFile, record{seq: seqNum, payload: data}); err != nil {
		return 0, fmt.Errorf("wal: write record: %w", err)
	}

	if err := fw.walFile.Sync(); err != nil {
		return 0, fmt.Errorf("wal: sync: %w", err)
	}

	return seqNum, nil
}

func (fw *FileWAL) Acknowledge(seqNum uint64) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	fw.acked[seqNum] = struct{}{}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, seqNum)
	if _, err := fw.ackFile.Write(buf); err != nil {
		return fmt.Errorf("wal: write ack: %w", err)
	}
	return nil
}

func (fw *FileWAL) Recover() ([]Entry, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	walPath := filepath.Join(fw.dir, walFileName)
	f, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal: open for recovery: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	var entries []Entry
	for {
		rec, err := readRecord(f)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			slog.Warn("WAL: truncated record at end of file, skipping")
			break
		}
		if err != nil {
			return nil, fmt.Errorf("wal: read record: %w", err)
		}

		if _, ok := fw.acked[rec.seq]; ok {
			continue
		}

		event := &pb.IngestRequest{}
		if err := proto.Unmarshal(rec.payload, event); err != nil {
			slog.Warn("WAL: corrupt record, skipping", "seq", rec.seq, "err", err)
			continue
		}

		entries = append(entries, Entry{SeqNum: rec.seq, Event: event})
	}

	return entries, nil
}

func (fw *FileWAL) Close() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	return errors.Join(
		fw.walFile.Sync(),
		fw.walFile.Close(),
		fw.ackFile.Sync(),
		fw.ackFile.Close(),
	)
}

func (fw *FileWAL) Checkpoint() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	walPath := filepath.Join(fw.dir, walFileName)
	f, err := os.Open(walPath)
	if err != nil {
		return fmt.Errorf("wal: open for checkpoint: %w", err)
	}

	var kept []record
	for {
		rec, err := readRecord(f)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			_ = f.Close()
			return fmt.Errorf("wal: checkpoint read record: %w", err)
		}
		if _, ok := fw.acked[rec.seq]; !ok {
			kept = append(kept, rec)
		}
	}
	_ = f.Close()

	tmpPath := walPath + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("wal: create tmp: %w", err)
	}

	for _, rec := range kept {
		if err := writeRecord(tmpFile, rec); err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("wal: write tmp record: %w", err)
		}
	}

	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("wal: sync tmp: %w", err)
	}
	_ = tmpFile.Close()

	_ = fw.walFile.Close()
	if err := os.Rename(tmpPath, walPath); err != nil {
		return fmt.Errorf("wal: rename tmp: %w", err)
	}

	fw.walFile, err = os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: reopen after checkpoint: %w", err)
	}

	_ = fw.ackFile.Close()
	ackPath := filepath.Join(fw.dir, ackFileName)
	fw.ackFile, err = os.OpenFile(ackPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: reopen ack after checkpoint: %w", err)
	}

	fw.acked = make(map[uint64]struct{})

	slog.Info("WAL checkpoint complete", "kept_entries", len(kept))
	return nil
}

func (fw *FileWAL) loadAcked() error {
	ackPath := filepath.Join(fw.dir, ackFileName)
	f, err := os.Open(ackPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	for {
		var buf [8]byte
		if _, err := io.ReadFull(f, buf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		seqNum := binary.BigEndian.Uint64(buf[:])
		fw.acked[seqNum] = struct{}{}
	}
	return nil
}

func (fw *FileWAL) loadMaxSeq() error {
	walPath := filepath.Join(fw.dir, walFileName)
	f, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	for {
		rec, err := readRecord(f)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return err
		}
		if rec.seq > fw.seq {
			fw.seq = rec.seq
		}
	}
	return nil
}
