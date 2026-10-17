package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Spool is a persistent, disk-backed FIFO queue for encoded API events.
// Run one replica against a given spool directory; it is not a distributed queue.
type Spool struct {
	dir      string
	maxItems int
	mu       sync.Mutex
	count    int
	changed  chan struct{} // Signals when capacity is freed
	wakeup   chan struct{} // Signals when an item is added
}

func NewSpool(dir string, maxItems int) (*Spool, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("spool directory cannot be empty")
	}
	if maxItems < 1 {
		return nil, fmt.Errorf("max spool items must be a positive integer")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create spool directory: %w", err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, fmt.Errorf("set spool directory permissions: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".failed"), 0700); err != nil {
		return nil, fmt.Errorf("create failed-event directory: %w", err)
	}

	files, err := listSpoolFiles(dir)
	if err != nil {
		return nil, fmt.Errorf("read spool directory: %w", err)
	}
	return &Spool{
		dir:      dir,
		maxItems: maxItems,
		count:    len(files),
		changed:  make(chan struct{}),
		wakeup:   make(chan struct{}, 1), // Buffered to prevent blocking Enqueue
	}, nil
}

func (s *Spool) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *Spool) Enqueue(ctx context.Context, eventID string, payload []byte) error {
	for {
		s.mu.Lock()
		if s.count < s.maxItems {
			s.count++ // Reserve capacity before writing, so concurrent producers cannot overfill.
			s.mu.Unlock()
			break
		}
		changed := s.changed
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}

	filename := fmt.Sprintf("event-%020d-%s.json", time.Now().UTC().UnixNano(), eventID)
	if err := writeAtomic(s.dir, filename, payload); err != nil {
		s.releaseSlot()
		return fmt.Errorf("persist event: %w", err)
	}

	// Signal the worker that a new item is available.
	select {
	case s.wakeup <- struct{}{}:
	default:
	}

	return nil
}

func writeAtomic(dir, filename string, payload []byte) error {
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(dir, filename)); err != nil {
		return err
	}
	ok = true
	return nil
}

func (s *Spool) RunWorker(ctx context.Context, deliverer *Deliverer) {
	failDir := filepath.Join(s.dir, ".failed")
	if err := os.MkdirAll(failDir, 0700); err != nil {
		log.Printf("cannot create failed-event directory: %v; delivery worker stopped", err)
		return
	}

	for ctx.Err() == nil {
		files, err := listSpoolFiles(s.dir)
		if err != nil {
			log.Printf("cannot list spool: %v", err)
			if !sleepContext(ctx, time.Second) {
				return
			}
			continue
		}
		if len(files) == 0 {
			// Wait for a new item to be enqueued, or a timeout to re-check directory state.
			select {
			case <-ctx.Done():
				return
			case <-s.wakeup:
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		for _, path := range files {
			if ctx.Err() != nil {
				return
			}
			s.processFile(ctx, deliverer, path, failDir)
		}
	}
}

func (s *Spool) processFile(ctx context.Context, deliverer *Deliverer, path, failDir string) {
	body, err := os.ReadFile(path)
	if err != nil {
		log.Printf("cannot read queued event %s: %v; moving to failed", filepath.Base(path), err)
		s.moveToFailed(path, failDir)
		s.releaseSlot()
		return
	}

	backoff := time.Second
	const maxRetries = 20
	retries := 0
	for ctx.Err() == nil {
		deliveryCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
		err = deliverer.Post(deliveryCtx, body)
		cancel()
		if err == nil {
			break
		}

		var apiErr *APIError
		if errors.As(err, &apiErr) && !apiErr.IsRetryable() {
			log.Printf("non-retryable API error for %s: %v; moving to failed", filepath.Base(path), err)
			s.moveToFailed(path, failDir)
			s.releaseSlot()
			return
		}

		retries++
		if retries > maxRetries {
			log.Printf("max retries reached for %s: %v; moving to failed", filepath.Base(path), err)
			s.moveToFailed(path, failDir)
			s.releaseSlot()
			return
		}

		log.Printf("delivery failed for queued event %s; retrying in %s: %v", filepath.Base(path), backoff, err)
		if !sleepContext(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}

	if ctx.Err() != nil {
		return
	}
	if err := os.Remove(path); err != nil {
		log.Printf("API accepted event but could not remove spool file %s: %v", filepath.Base(path), err)
		s.moveToFailed(path, failDir) // Keep accepted events out of the active queue when possible.
	}
	s.releaseSlot()
	log.Printf("delivered and removed queued event %s", filepath.Base(path))
}

func (s *Spool) moveToFailed(path, failDir string) {
	base := filepath.Base(path)
	dest := filepath.Join(failDir, base)
	if err := os.Rename(path, dest); err != nil {
		log.Printf("failed to move %s to failed directory: %v", base, err)
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			log.Printf("failed to remove unmovable spool file %s: %v", base, removeErr)
		}
	}
}

func (s *Spool) releaseSlot() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count > 0 {
		s.count--
	}
	close(s.changed)
	s.changed = make(chan struct{})
}

func listSpoolFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "event-") && strings.HasSuffix(entry.Name(), ".json") {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

func nextBackoff(current time.Duration) time.Duration {
	current *= 2
	if current > 60*time.Second {
		return 60 * time.Second
	}
	return current
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
