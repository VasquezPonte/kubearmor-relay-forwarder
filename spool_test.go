package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSpoolEnqueuePersistsEvent(t *testing.T) {
	s := newTestSpool(t, 2)
	payload := []byte(`{"eventId":"abc"}`)
	if err := s.Enqueue(context.Background(), "abc", payload); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if got := s.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1", got)
	}
	files, err := listSpoolFiles(s.dir)
	if err != nil {
		t.Fatalf("listSpoolFiles() error = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("found %d files, want 1", len(files))
	}
	got, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("file payload = %q, want %q", got, payload)
	}
}

func TestSpoolEnqueueWaitsForCapacityAndHonorsCancellation(t *testing.T) {
	s := newTestSpool(t, 1)
	if err := s.Enqueue(context.Background(), "first", []byte("first")); err != nil {
		t.Fatalf("first Enqueue() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := s.Enqueue(ctx, "second", []byte("second"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Enqueue() error = %v, want context deadline exceeded", err)
	}
	if got := s.Count(); got != 1 {
		t.Fatalf("Count() = %d after canceled enqueue, want 1", got)
	}
}

func TestAPIErrorRetryability(t *testing.T) {
	cases := []struct {
		status int
		want   bool
	}{
		{http.StatusBadRequest, false},
		{http.StatusUnauthorized, false},
		{http.StatusRequestTimeout, true},
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
	}
	for _, tc := range cases {
		err := &APIError{StatusCode: tc.status}
		if got := err.IsRetryable(); got != tc.want {
			t.Errorf("status %d: IsRetryable() = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestNextBackoffCapsAtOneMinute(t *testing.T) {
	if got := nextBackoff(40 * time.Second); got != 60*time.Second {
		t.Fatalf("nextBackoff(40s) = %s, want 1m", got)
	}
}

func newTestSpool(t *testing.T, maxItems int) *Spool {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "spool")
	s, err := NewSpool(dir, maxItems)
	if err != nil {
		t.Fatalf("NewSpool() error = %v", err)
	}
	return s
}
