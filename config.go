package main

import (
	"encoding/json"
	"errors"
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
		return Config{}, errors.New("MAX_SPOOL_EVENTS must be a positive integer")
	}
	cfg.MaxSpoolItems = maxItems

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}

	customFields, err := parseCustomFields()
	if err != nil {
		return Config{}, err
	}
	cfg.CustomFields = customFields

	return cfg, nil
}

// validate checks the string settings in the order they are reported to the user.
func (c Config) validate() error {
	switch {
		case c.RelayAddr == "":
			return errors.New("RELAY_ADDR cannot be empty")
		case c.HealthAddr == "":
			return errors.New("HEALTH_ADDR cannot be empty")
		case strings.TrimSpace(c.APIKey) == "":
			return errors.New("API_KEY must be set through a Kubernetes Secret")
		case strings.ContainsAny(c.APIKey, "\r\n"):
			return errors.New("API_KEY must not contain newline characters")
		case strings.ContainsAny(c.APIKeyPrefix, "\r\n"):
			return errors.New("API_KEY_PREFIX must not contain newline characters")
		case c.APIKeyHeader == "":
			return errors.New("API_KEY_HEADER cannot be empty")
		case !validHeaderName(c.APIKeyHeader):
			return errors.New("API_KEY_HEADER must be a valid HTTP header name")
	}
	return validateAPIURL(c.APIURL)
}

func validateAPIURL(rawURL string) error {
	if rawURL == "" {
		return errors.New("API_URL must be set to the destination API's absolute HTTP(S) URL")
	}

	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("API_URL must be an absolute HTTP(S) URL")
	}
	if u.Scheme != "https" && env("ALLOW_INSECURE_API_HTTP", "false") != "true" {
		return errors.New("API_URL must use HTTPS (set ALLOW_INSECURE_API_HTTP=true only for controlled testing)")
	}
	return nil
}

// parseCustomFields reads CUSTOM_FIELDS_JSON. A blank value yields no custom fields.
func parseCustomFields() (map[string]any, error) {
	raw := env("CUSTOM_FIELDS_JSON", `{"managedBy":"flux"}`)
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	var fields map[string]any
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return nil, fmt.Errorf("CUSTOM_FIELDS_JSON must be a JSON object: %w", err)
	}
	if fields == nil {
		return nil, errors.New("CUSTOM_FIELDS_JSON must be a JSON object, not null")
	}
	return fields, nil
}

// env returns the value of key if it is set (even if empty), otherwise fallback.
func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// validHeaderName reports whether name is a valid HTTP header field name (an RFC 7230 token).
func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isHeaderTokenChar(name[i]) {
			return false
		}
	}
	return true
}

func isHeaderTokenChar(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c))
}
