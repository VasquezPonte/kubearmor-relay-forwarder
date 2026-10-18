package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	userAgent         = "kubearmor-relay-forwarder/1.0.0"
	maxErrorBodyBytes = 1024
	maxDrainBodyBytes = 4096
)

// APIError describes a non-2xx response from the destination.
type APIError struct {
	StatusCode int
	Status     string
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API returned %s: %s", e.Status, e.Message)
}

// IsRetryable reports HTTP statuses that are normally transient.
func (e *APIError) IsRetryable() bool {
	return e.StatusCode == http.StatusRequestTimeout ||
		e.StatusCode == http.StatusTooManyRequests ||
		e.StatusCode >= 500
}

// Deliverer posts encoded events to the configured API.
type Deliverer struct {
	Client *http.Client
	Config Config
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          20,
			MaxIdleConnsPerHost:   4,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 15 * time.Second,
			ForceAttemptHTTP2:     true,
		},
	}
}

// Post sends body to the API. A non-2xx response is returned as an *APIError.
func (d *Deliverer) Post(ctx context.Context, body []byte) error {
	req, err := d.newRequest(ctx, body)
	if err != nil {
		return err
	}

	resp, err := d.Client.Do(req)
	if err != nil {
		return fmt.Errorf("send API request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		return &APIError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Message:    strings.TrimSpace(string(message)),
		}
	}

	// Drain a bounded amount of the body so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBodyBytes))

	return nil
}

func (d *Deliverer) newRequest(ctx context.Context, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.Config.APIURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create API request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)

	// Custom headers (WEBHOOK_CUSTOMHEADERS) are applied last, so they can
	// also override the defaults above if configured to do so.
	for name, value := range d.Config.CustomHeaders {
		req.Header.Set(name, value)
	}

	return req, nil
}
