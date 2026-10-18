package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	dirMode         = 0o700
	failedDirName   = ".failed"
	eventFilePrefix = "event-"
	eventFileSuffix = ".json"

	maxDeliveryRetries  = 20
	deliveryTimeout     = 25 * time.Second
	idlePollInterval    = 500 * time.Millisecond
	spoolListRetryDelay = time.Second
)

// Spool is a persistent, disk-backed FIFO queue for encoded API events.
// Run one replica against a given spool directory; it is not a distributed queue.
type Spool struct {
	dir       string
	failedDir string
	maxItems  int

	mu            sync.Mutex    // guards count and capacityFreed
	count         int           // queued events, including slots reserved by in-flight Enqueue calls
	capacityFreed chan struct{} // closed and replaced whenever a slot is released
	itemAdded     chan struct{} // buffered; signals the worker that an item was enqueued
}

func NewSpool(dir string, maxItems int) (*Spool, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("spool directory cannot be empty")
	}
	if maxItems < 1 {
		return nil, errors.New("max spool items must be a positive integer")
	}

	failedDir := filepath.Join(dir, failedDirName)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("create spool directory: %w", err)
	}
	if err := os.Chmod(dir, dirMode); err != nil {
		return nil, fmt.Errorf("set spool directory permissions: %w", err)
	}
	if err := os.MkdirAll(failedDir, dirMode); err != nil {
		return nil, fmt.Errorf("create failed-event directory: %w", err)
	}

	files, err := listSpoolFiles(dir)
	if err != nil {
		return nil, fmt.Errorf("read spool directory: %w", err)
	}
	return &Spool{
		dir:           dir,
		failedDir:     failedDir,
		maxItems:      maxItems,
		count:         len(files),
		capacityFreed: make(chan struct{}),
		itemAdded:     make(chan struct{}, 1), // Buffered to prevent blocking Enqueue
	}, nil
}

func (s *Spool) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// Enqueue persists payload, blocking while the spool is full until capacity
// frees up or ctx is done.
func (s *Spool) Enqueue(ctx context.Context, eventID string, payload []byte) error {
	if err := s.reserveSlot(ctx); err != nil {
		return err
	}

	filename := fmt.Sprintf("%s%020d-%s%s", eventFilePrefix, time.Now().UTC().UnixNano(), eventID, eventFileSuffix)
	if err := writeAtomic(s.dir, filename, payload); err != nil {
		s.releaseSlot()
		return fmt.Errorf("persist event: %w", err)
	}

	// Signal the worker that a new item is available.
	select {
	case s.itemAdded <- struct{}{}:
	default:
	}

	return nil
}

// reserveSlot waits for free capacity and claims it before anything is written,
// so concurrent producers cannot overfill the spool.
func (s *Spool) reserveSlot(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.count < s.maxItems {
			s.count++
			s.mu.Unlock()
			return nil
		}
		freed := s.capacityFreed
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-freed:
		}
	}
}

func (s *Spool) releaseSlot() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count > 0 {
		s.count--
	}
	close(s.capacityFreed)
	s.capacityFreed = make(chan struct{})
}

// writeAtomic writes payload to dir/filename via a synced temporary file and a
// rename, removing the temporary file on failure.
func writeAtomic(dir, filename string, payload []byte) (err error) {
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp.Name())
		}
	}()

	if err = writeSynced(tmp, payload); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, filename))
}

// writeSynced writes payload to f, syncs it to disk, and closes it.
func writeSynced(f *os.File, payload []byte) error {
	if _, err := f.Write(payload); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// RunWorker delivers spooled events in order until ctx is done.
func (s *Spool) RunWorker(ctx context.Context, deliverer *Deliverer) {
	if err := os.MkdirAll(s.failedDir, dirMode); err != nil {
		log.Printf("cannot create failed-event directory: %v; delivery worker stopped", err)
		return
	}

	for ctx.Err() == nil {
		files, err := listSpoolFiles(s.dir)
		if err != nil {
			log.Printf("cannot list spool: %v", err)
			if !sleepContext(ctx, spoolListRetryDelay) {
				return
			}
			continue
		}
		if len(files) == 0 {
			s.waitForItem(ctx)
			continue
		}

		for _, path := range files {
			if ctx.Err() != nil {
				return
			}
			s.processFile(ctx, deliverer, path)
		}
	}
}

// waitForItem blocks until an item is enqueued, ctx is done, or a short timeout
// elapses so the directory state is re-checked.
func (s *Spool) waitForItem(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-s.itemAdded:
	case <-time.After(idlePollInterval):
	}
}

type deliveryOutcome int

const (
	outcomeDelivered deliveryOutcome = iota
	outcomeFailed
	outcomeInterrupted
)

func (s *Spool) processFile(ctx context.Context, deliverer *Deliverer, path string) {
	name := filepath.Base(path)

	body, err := os.ReadFile(path)
	if err != nil {
		log.Printf("cannot read queued event %s: %v; moving to failed", name, err)
		s.failEvent(path)
		return
	}

	switch deliverWithRetry(ctx, deliverer, name, body) {
	case outcomeDelivered:
		s.completeEvent(path)
	case outcomeFailed:
		s.failEvent(path)
	case outcomeInterrupted:
		// Leave the file in place; it is redelivered on the next run.
	}
}

// deliverWithRetry posts body until it is accepted, fails permanently, or ctx is done.
func deliverWithRetry(ctx context.Context, deliverer *Deliverer, name string, body []byte) deliveryOutcome {
	backoff := initialBackoff
	retries := 0
	for ctx.Err() == nil {
		err := postWithTimeout(ctx, deliverer, body)
		if err == nil {
			break
		}

		var apiErr *APIError
		if errors.As(err, &apiErr) && !apiErr.IsRetryable() {
			log.Printf("non-retryable API error for %s: %v; moving to failed", name, err)
			return outcomeFailed
		}

		retries++
		if retries > maxDeliveryRetries {
			log.Printf("max retries reached for %s: %v; moving to failed", name, err)
			return outcomeFailed
		}

		log.Printf("delivery failed for queued event %s; retrying in %s: %v", name, backoff, err)
		if !sleepContext(ctx, backoff) {
			return outcomeInterrupted
		}
		backoff = nextBackoff(backoff)
	}

	if ctx.Err() != nil {
		return outcomeInterrupted
	}
	return outcomeDelivered
}

func postWithTimeout(ctx context.Context, deliverer *Deliverer, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()
	return deliverer.Post(ctx, body)
}

// completeEvent removes a delivered event from the spool and frees its slot.
func (s *Spool) completeEvent(path string) {
	name := filepath.Base(path)
	if err := os.Remove(path); err != nil {
		log.Printf("API accepted event but could not remove spool file %s: %v", name, err)
		s.moveToFailed(path) // keep accepted events out of the active queue when possible.
	}
	s.releaseSlot()
	log.Printf("delivered and removed queued event %s", name)
}

// failEvent moves an undeliverable event out of the active queue and frees its slot.
func (s *Spool) failEvent(path string) {
	s.moveToFailed(path)
	s.releaseSlot()
}

func (s *Spool) moveToFailed(path string) {
	base := filepath.Base(path)
	dest := filepath.Join(s.failedDir, base)
	if err := os.Rename(path, dest); err != nil {
		log.Printf("failed to move %s to failed directory: %v", base, err)
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			log.Printf("failed to remove unmovable spool file %s: %v", base, removeErr)
		}
	}
}

// listSpoolFiles returns the queued event files in dir in filename order.
// os.ReadDir already sorts entries by filename, and filenames embed a
// zero-padded timestamp, so this is oldest-first.
func listSpoolFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && isSpoolFileName(entry.Name()) {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	return files, nil
}

func isSpoolFileName(name string) bool {
	return strings.HasPrefix(name, eventFilePrefix) && strings.HasSuffix(name, eventFileSuffix)
}
