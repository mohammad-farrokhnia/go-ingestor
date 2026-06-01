package wal

import "sync"

type SeqTracker struct {
	mu   sync.RWMutex
	data map[string]uint64
}

func NewSeqTracker() *SeqTracker {
	return &SeqTracker{
		data: make(map[string]uint64),
	}
}

func (t *SeqTracker) Store(eventID string, seqNum uint64) {
	t.mu.Lock()
	t.data[eventID] = seqNum
	t.mu.Unlock()
}

func (t *SeqTracker) LoadAndDelete(eventID string) (uint64, bool) {
	t.mu.Lock()
	seq, ok := t.data[eventID]
	if ok {
		delete(t.data, eventID)
	}
	t.mu.Unlock()
	return seq, ok
}

func (t *SeqTracker) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.data)
}
