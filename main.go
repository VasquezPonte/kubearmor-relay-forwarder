package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const healthShutdownTimeout = 5 * time.Second

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lmicroseconds)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	spool, err := NewSpool(cfg.SpoolDir, cfg.MaxSpoolItems)
	if err != nil {
		log.Fatalf("initialize spool: %v", err)
	}
	log.Printf("loaded %d pending event(s) from spool", spool.Count())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var relayReady atomic.Bool
	deliverer := &Deliverer{
		Client: newHTTPClient(),
		Config: cfg,
	}

	healthServer := newHealthServer(cfg.HealthAddr, &relayReady)
	healthErr := serveHealth(healthServer, stop)

	var workers sync.WaitGroup
	goTracked(&workers, func() { spool.RunWorker(ctx, deliverer) })
	goTracked(&workers, func() { WatchAlerts(ctx, cfg, spool, &relayReady) })

	select {
	case <-ctx.Done():
	case err := <-healthErr:
		log.Printf("shutting down after health server error: %v", err)
	}
	relayReady.Store(false)

	shutdownHealthServer(healthServer)

	workers.Wait()
	log.Printf("forwarder stopped")
}

// serveHealth runs srv in the background. If it fails, the error is sent on the
// returned channel and stop is called to begin shutdown.
func serveHealth(srv *http.Server, stop context.CancelFunc) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("health server failed: %v", err)
			errCh <- err
			stop()
		}
	}()
	return errCh
}

func shutdownHealthServer(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), healthShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("health server shutdown error: %v", err)
	}
}

// goTracked runs fn in a new goroutine that is tracked by wg.
func goTracked(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		fn()
	}()
}
