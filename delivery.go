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
		e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
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

func (d *Deliverer) Post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.Config.APIURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create API request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "kubearmor-relay-forwarder/1.0.0")
	req.Header.Set("X-Security-Source", "kubearmor")
	req.Header.Set(d.Config.APIKeyHeader, d.Config.APIKeyPrefix+d.Config.APIKey)

	resp, err := d.Client.Do(req)
	if err != nil {
		return fmt.Errorf("send API request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return &APIError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Message:    strings.TrimSpace(string(message)),
		}
	}

	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return nil
}
