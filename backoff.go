package main

import (
	"context"
	"time"
)

const (
	initialBackoff = time.Second
	maxBackoff     = 60 * time.Second
)

// nextBackoff doubles current, capped at maxBackoff.
func nextBackoff(current time.Duration) time.Duration {
	current *= 2
	if current > maxBackoff {
		return maxBackoff
	}
	return current
}

// sleepContext waits for duration and reports whether it completed before ctx was done.
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
