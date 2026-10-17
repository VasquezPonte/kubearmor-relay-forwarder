package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Config contains the runtime configuration, supplied through environment variables.
type Config struct {
	RelayAddr     string
	APIURL        string
	APIKey        string
	APIKeyHeader  string
	APIKeyPrefix  string
	Source        string
	CustomFields  map[string]any
	SpoolDir      string
	MaxSpoolItems int
	HealthAddr    string
}

func loadConfig() (Config, error) {
	cfg := Config{
		RelayAddr:    strings.TrimSpace(env("RELAY_ADDR", "kubearmor.kubearmor.svc.cluster.local:32767")),
		APIURL:       strings.TrimSpace(os.Getenv("API_URL")),
		APIKey:       os.Getenv("API_KEY"),
		APIKeyHeader: strings.TrimSpace(env("API_KEY_HEADER", "Authorization")),
		APIKeyPrefix: env("API_KEY_PREFIX", "Bearer "),
		Source:       env("SOURCE_NAME", "kubearmor"),
		SpoolDir:     env("SPOOL_DIR", "/var/lib/kubearmor-forwarder/spool"),
		HealthAddr:   strings.TrimSpace(env("HEALTH_ADDR", ":8080")),
	}

	maxItems, err := strconv.Atoi(env("MAX_SPOOL_EVENTS", "20000"))
	if err != nil || maxItems < 1 {
		return Config{}, fmt.Errorf("MAX_SPOOL_EVENTS must be a positive integer")
	}
	cfg.MaxSpoolItems = maxItems

	if cfg.RelayAddr == "" {
		return Config{}, fmt.Errorf("RELAY_ADDR cannot be empty")
	}
	if cfg.HealthAddr == "" {
		return Config{}, fmt.Errorf("HEALTH_ADDR cannot be empty")
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return Config{}, fmt.Errorf("API_KEY must be set through a Kubernetes Secret")
	}
	if strings.ContainsAny(cfg.APIKey, "\r\n") {
		return Config{}, fmt.Errorf("API_KEY must not contain newline characters")
	}
	if strings.ContainsAny(cfg.APIKeyPrefix, "\r\n") {
		return Config{}, fmt.Errorf("API_KEY_PREFIX must not contain newline characters")
	}
	if cfg.APIKeyHeader == "" {
		return Config{}, fmt.Errorf("API_KEY_HEADER cannot be empty")
	}
	if !validHeaderName(cfg.APIKeyHeader) {
		return Config{}, fmt.Errorf("API_KEY_HEADER must be a valid HTTP header name")
	}
	if cfg.APIURL == "" {
		return Config{}, fmt.Errorf("API_URL must be set to the destination API's absolute HTTP(S) URL")
	}

	u, err := url.Parse(cfg.APIURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return Config{}, fmt.Errorf("API_URL must be an absolute HTTP(S) URL")
	}
	if u.Scheme != "https" && env("ALLOW_INSECURE_API_HTTP", "false") != "true" {
		return Config{}, fmt.Errorf("API_URL must use HTTPS (set ALLOW_INSECURE_API_HTTP=true only for controlled testing)")
	}

	raw := env("CUSTOM_FIELDS_JSON", `{"managedBy":"flux"}`)
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &cfg.CustomFields); err != nil {
			return Config{}, fmt.Errorf("CUSTOM_FIELDS_JSON must be a JSON object: %w", err)
		}
		if cfg.CustomFields == nil {
			return Config{}, fmt.Errorf("CUSTOM_FIELDS_JSON must be a JSON object, not null")
		}
	}

	return cfg, nil
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c)) {
			continue
		}
		return false
	}
	return true
}
