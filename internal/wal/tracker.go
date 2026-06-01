package wal

import "sync"

// SeqTracker maps event IDs to their WAL sequence numbers.
// Used to look up seqNums when acknowledging events after successful sink writes.
type SeqTracker struct {
	mu   sync.RWMutex
	data map[string]uint64
}

func NewSeqTracker() *SeqTracker {
	return &SeqTracker{
		data: make(map[string]uint64),
	}
}

// Store records the mapping from event_id to WAL sequence number.
func (t *SeqTracker) Store(eventID string, seqNum uint64) {
	t.mu.Lock()
	t.data[eventID] = seqNum
	t.mu.Unlock()
}

// LoadAndDelete retrieves and removes the seqNum for the given event_id.
// Returns 0, false if not found.
func (t *SeqTracker) LoadAndDelete(eventID string) (uint64, bool) {
	t.mu.Lock()
	seq, ok := t.data[eventID]
	if ok {
		delete(t.data, eventID)
	}
	t.mu.Unlock()
	return seq, ok
}

// Len returns the number of tracked entries.
func (t *SeqTracker) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.data)
}
