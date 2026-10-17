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
	healthErr := make(chan error, 1)
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("health server failed: %v", err)
			healthErr <- err
			stop()
		}
	}()

	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		spool.RunWorker(ctx, deliverer)
	}()
	go func() {
		defer workers.Done()
		WatchAlerts(ctx, cfg, spool, &relayReady)
	}()

	select {
	case <-ctx.Done():
	case err := <-healthErr:
		log.Printf("shutting down after health server error: %v", err)
	}
	relayReady.Store(false)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("health server shutdown error: %v", err)
	}

	workers.Wait()
	log.Printf("forwarder stopped")
}
