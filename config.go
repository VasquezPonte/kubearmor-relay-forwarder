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
	RelayAddr         string
	APIURL            string
	CustomHeaders     map[string]string
	Source            string
	CustomFields      map[string]any
	SpoolDir          string
	MaxSpoolItems     int
	HealthAddr        string
	AllowInsecureHTTP bool
}

func loadConfig() (Config, error) {
	cfg := Config{
		RelayAddr:  strings.TrimSpace(env("RELAY_ADDR", "kubearmor.kubearmor.svc.cluster.local:32767")),
		APIURL:     strings.TrimSpace(os.Getenv("API_URL")),
		Source:     env("SOURCE_NAME", "kubearmor"),
		SpoolDir:   env("SPOOL_DIR", "/var/lib/kubearmor-forwarder/spool"),
		HealthAddr: strings.TrimSpace(env("HEALTH_ADDR", ":8080")),
	}

	maxItems, err := strconv.Atoi(env("MAX_SPOOL_EVENTS", "20000"))
	if err != nil || maxItems < 1 {
		return Config{}, errors.New("MAX_SPOOL_EVENTS must be a positive integer")
	}
	cfg.MaxSpoolItems = maxItems

	allowInsecure, err := parseBoolEnv("ALLOW_INSECURE_API_HTTP", false)
	if err != nil {
		return Config{}, err
	}
	cfg.AllowInsecureHTTP = allowInsecure

	customHeaders, err := parseCustomHeaders(os.Getenv("WEBHOOK_CUSTOMHEADERS"))
	if err != nil {
		return Config{}, err
	}
	cfg.CustomHeaders = customHeaders

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
	case len(c.CustomHeaders) == 0:
		return errors.New("WEBHOOK_CUSTOMHEADERS must contain at least one header")
	}

	return validateAPIURL(c.APIURL, c.AllowInsecureHTTP)
}

// parseCustomHeaders reads comma-separated Header:Value pairs.
// Header values may contain colons, but commas are treated as separators.
func parseCustomHeaders(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("WEBHOOK_CUSTOMHEADERS must be set")
	}

	headers := make(map[string]string)
	seen := make(map[string]struct{})

	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			//return nil, errors.New("WEBHOOK_CUSTOMHEADERS contains an empty header entry")
			continue // tolerate trailing or doubled commas
		}

		name, value, ok := strings.Cut(entry, ":")
		if !ok {
			return nil, fmt.Errorf("WEBHOOK_CUSTOMHEADERS entry %q must use Header:Value format", entry)
		}

		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)

		if !validHeaderName(name) {
			return nil, fmt.Errorf(
				"WEBHOOK_CUSTOMHEADERS contains invalid header name %q", name)
		}

		if strings.ContainsAny(value, "\r\n") {
			return nil, fmt.Errorf("WEBHOOK_CUSTOMHEADERS value for %q must not contain newline characters", name)
		}

		// HTTP header names are case-insensitive. Reject duplicates to avoid
		// ambiguous values and nondeterministic behavior when applying the map.
		key := strings.ToLower(name)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("WEBHOOK_CUSTOMHEADERS contains duplicate header %q", name)
		}

		seen[key] = struct{}{}
		headers[name] = value
	}

	if len(headers) == 0 {
		return nil, errors.New("WEBHOOK_CUSTOMHEADERS must contain at least one header")
	}

	return headers, nil
}

func validateAPIURL(rawURL string, allowInsecure bool) error {
	if rawURL == "" {
		return errors.New("API_URL must be set to the destination API's absolute HTTP(S) URL")
	}

	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("API_URL must be an absolute HTTP(S) URL")
	}
	if u.Scheme != "https" && !allowInsecure {
		return errors.New("API_URL must use HTTPS (set ALLOW_INSECURE_API_HTTP=true only for controlled testing)")
	}

	return nil
}

// parseBoolEnv reads a boolean environment variable. An unset or blank value yields fallback.
func parseBoolEnv(key string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(env(key, ""))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", key)
	}

	return value, nil
}

// parseCustomFields reads CUSTOM_FIELDS_JSON.
// An unset or blank value yields no custom fields.
func parseCustomFields() (map[string]any, error) {
	raw := env("CUSTOM_FIELDS_JSON", "")
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
