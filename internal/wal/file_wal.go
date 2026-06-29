package wal

import (
	"encoding/binary"
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

	header := make([]byte, 12)
	binary.BigEndian.PutUint64(header[0:8], seqNum)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(data)))

	if _, err := fw.walFile.Write(header); err != nil {
		return 0, fmt.Errorf("wal: write header: %w", err)
	}
	if _, err := fw.walFile.Write(data); err != nil {
		return 0, fmt.Errorf("wal: write payload: %w", err)
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
		var header [12]byte
		if _, err := io.ReadFull(f, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("wal: read header: %w", err)
		}

		seqNum := binary.BigEndian.Uint64(header[0:8])
		length := binary.BigEndian.Uint32(header[8:12])

		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				slog.Warn("WAL: truncated record at end of file, skipping", "seq", seqNum)
				break
			}
			return nil, fmt.Errorf("wal: read payload: %w", err)
		}

		if _, ok := fw.acked[seqNum]; ok {
			continue
		}

		event := &pb.IngestRequest{}
		if err := proto.Unmarshal(payload, event); err != nil {
			slog.Warn("WAL: corrupt record, skipping", "seq", seqNum, "err", err)
			continue
		}

		entries = append(entries, Entry{SeqNum: seqNum, Event: event})
	}

	return entries, nil
}

func (fw *FileWAL) Close() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	var errs []error
	if err := fw.walFile.Sync(); err != nil {
		errs = append(errs, err)
	}
	if err := fw.walFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := fw.ackFile.Sync(); err != nil {
		errs = append(errs, err)
	}
	if err := fw.ackFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}


func (fw *FileWAL) Checkpoint() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	walPath := filepath.Join(fw.dir, walFileName)
	f, err := os.Open(walPath)
	if err != nil {
		return fmt.Errorf("wal: open for checkpoint: %w", err)
	}

	var kept []struct {
		seqNum  uint64
		payload []byte
	}

	for {
		var header [12]byte
		if _, err := io.ReadFull(f, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			_ = f.Close()
			return fmt.Errorf("wal: checkpoint read header: %w", err)
		}

		seqNum := binary.BigEndian.Uint64(header[0:8])
		length := binary.BigEndian.Uint32(header[8:12])

		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			break
		}

		if _, ok := fw.acked[seqNum]; !ok {
			kept = append(kept, struct {
				seqNum  uint64
				payload []byte
			}{seqNum: seqNum, payload: payload})
		}
	}
	_ = f.Close()

	tmpPath := walPath + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("wal: create tmp: %w", err)
	}

	for _, entry := range kept {
		header := make([]byte, 12)
		binary.BigEndian.PutUint64(header[0:8], entry.seqNum)
		binary.BigEndian.PutUint32(header[8:12], uint32(len(entry.payload)))
		if _, err := tmpFile.Write(header); err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("wal: write tmp: %w", err)
		}
		if _, err := tmpFile.Write(entry.payload); err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("wal: write tmp payload: %w", err)
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
		var header [12]byte
		if _, err := io.ReadFull(f, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		seqNum := binary.BigEndian.Uint64(header[0:8])
		length := binary.BigEndian.Uint32(header[8:12])

		if seqNum > fw.seq {
			fw.seq = seqNum
		}

		if _, err := f.Seek(int64(length), io.SeekCurrent); err != nil {
			return err
		}
	}
	return nil
}
