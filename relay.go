package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	pb "github.com/kubearmor/KubeArmor/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func WatchAlerts(ctx context.Context, cfg Config, spool *Spool, ready *atomic.Bool) {
	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := streamAlerts(ctx, cfg, spool, ready)
		ready.Store(false)

		if ctx.Err() != nil {
			return
		}
		if time.Since(started) >= 30*time.Second {
			backoff = time.Second
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

	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = waitForRelayReady(connectCtx, conn)
	cancel()
	if err != nil {
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

		body, eventID, err := makePayload(alert, cfg)
		if err != nil {
			log.Printf("cannot encode Relay alert: %v", err)
			continue
		}

		for {
			if err := spool.Enqueue(ctx, eventID, body); err == nil {
				break
			} else if ctx.Err() != nil {
				return ctx.Err()
			} else {
				log.Printf("could not persist event %s to spool; retrying: %v", eventID, err)
			}
			if !sleepContext(ctx, time.Second) {
				return ctx.Err()
			}
		}
	}
}

func waitForRelayReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return fmt.Errorf("gRPC connection shut down before becoming ready")
		}
		if !conn.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("gRPC connection state did not change")
		}
	}
}
