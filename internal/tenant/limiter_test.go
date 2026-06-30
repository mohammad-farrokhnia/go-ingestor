package tenant

import (
	"testing"
	"time"
)

func TestTokenBucket_NilAlwaysAllows(t *testing.T) {
	var b *tokenBucket
	for i := 0; i < 100; i++ {
		if !b.allow() {
			t.Fatal("nil bucket must always allow")
		}
	}
}

func TestTokenBucket_BurstThenDeny(t *testing.T) {
	b := newTokenBucket(5) // burst = 5
	now := time.Unix(0, 0)
	b.now = func() time.Time { return now }
	b.last = now

	for i := 0; i < 5; i++ {
		if !b.allow() {
			t.Fatalf("call %d should be admitted within burst", i+1)
		}
	}
	if b.allow() {
		t.Fatal("call beyond burst should be denied")
	}
}

func TestTokenBucket_RefillsOverTime(t *testing.T) {
	b := newTokenBucket(10)
	now := time.Unix(0, 0)
	b.now = func() time.Time { return now }
	b.last = now

	for i := 0; i < 10; i++ {
		if !b.allow() {
			t.Fatalf("call %d should be admitted", i+1)
		}
	}
	if b.allow() {
		t.Fatal("bucket should be empty")
	}

	now = now.Add(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if !b.allow() {
			t.Fatalf("refilled call %d should be admitted", i+1)
		}
	}
	if b.allow() {
		t.Fatal("only 5 tokens should have refilled")
	}
}

func TestTokenBucket_CapsAtBurst(t *testing.T) {
	b := newTokenBucket(4)
	now := time.Unix(0, 0)
	b.now = func() time.Time { return now }
	b.last = now

	now = now.Add(1 * time.Hour)
	count := 0
	for b.allow() {
		count++
		if count > 100 {
			t.Fatal("tokens accumulated beyond burst")
		}
	}
	if count != 4 {
		t.Fatalf("expected burst of 4, got %d", count)
	}
}
