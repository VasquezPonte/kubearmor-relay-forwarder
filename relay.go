package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	pb "github.com/kubearmor/KubeArmor/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	relayConnectTimeout = 10 * time.Second
	relayStableAfter    = 30 * time.Second // stream lifetime after which the reconnect backoff resets
	spoolEnqueueRetry   = time.Second
)

// WatchAlerts keeps a Relay alert stream open, reconnecting with backoff until ctx is done.
func WatchAlerts(ctx context.Context, cfg Config, spool *Spool, ready *atomic.Bool) {
	backoff := initialBackoff
	for ctx.Err() == nil {
		started := time.Now()
		err := streamAlerts(ctx, cfg, spool, ready)
		ready.Store(false)

		if ctx.Err() != nil {
			return
		}
		if time.Since(started) >= relayStableAfter {
			backoff = initialBackoff
		}

		log.Printf("KubeArmor Relay stream ended: %v; retrying in %s", err, backoff)
		if !sleepContext(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

func streamAlerts(ctx context.Context, cfg Config, spool *Spool, ready *atomic.Bool) error {
	// KubeArmor Relay's in-cluster endpoint is commonly plaintext gRPC. Use TLS
	// credentials here if the Relay endpoint is configured to require TLS.
	conn, err := grpc.NewClient(cfg.RelayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create Relay client: %w", err)
	}
	defer conn.Close()

	if err := connectRelay(ctx, conn); err != nil {
		return fmt.Errorf("connect to Relay: %w", err)
	}

	client := pb.NewLogServiceClient(conn)
	stream, err := client.WatchAlerts(ctx, &pb.RequestMessage{Filter: "all"})
	if err != nil {
		return fmt.Errorf("subscribe to alerts: %w", err)
	}

	ready.Store(true)
	log.Printf("subscribed to KubeArmor Relay alerts at %s", cfg.RelayAddr)

	for {
		alert, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receive Relay alert: %w", err)
		}

		if err := forwardAlert(ctx, alert, cfg, spool); err != nil {
			return err
		}
	}
}

// forwardAlert encodes alert and persists it to the spool. Alerts that cannot be
// encoded are logged and skipped; the only error returned is ctx cancellation.
func forwardAlert(ctx context.Context, alert *pb.Alert, cfg Config, spool *Spool) error {
	body, eventID, err := makePayload(alert, cfg)
	if err != nil {
		log.Printf("cannot encode Relay alert: %v", err)
		return nil
	}
	return enqueueUntilPersisted(ctx, spool, eventID, body)
}

// enqueueUntilPersisted retries spool.Enqueue until it succeeds or ctx is done.
func enqueueUntilPersisted(ctx context.Context, spool *Spool, eventID string, body []byte) error {
	for {
		err := spool.Enqueue(ctx, eventID, body)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("could not persist event %s to spool; retrying: %v", eventID, err)
		if !sleepContext(ctx, spoolEnqueueRetry) {
			return ctx.Err()
		}
	}
}

func connectRelay(ctx context.Context, conn *grpc.ClientConn) error {
	ctx, cancel := context.WithTimeout(ctx, relayConnectTimeout)
	defer cancel()
	return waitForRelayReady(ctx, conn)
}

func waitForRelayReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return errors.New("gRPC connection shut down before becoming ready")
		}
		if !conn.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return errors.New("gRPC connection state did not change")
		}
	}
}
