package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/kubearmor/KubeArmor/protobuf"
)

// APIEvent is the JSON envelope sent to the destination API.
type APIEvent struct {
	EventID     string          `json:"eventId"`
	Source      string          `json:"source"`
	ReceivedAt  time.Time       `json:"receivedAt"`
	EventType   string          `json:"eventType"`
	Severity    string          `json:"severity,omitempty"`
	Namespace   string          `json:"namespace,omitempty"`
	Pod         string          `json:"pod,omitempty"`
	Host        string          `json:"host,omitempty"`
	Policy      string          `json:"policy,omitempty"`
	Operation   string          `json:"operation,omitempty"`
	Action      string          `json:"action,omitempty"`
	Result      string          `json:"result,omitempty"`
	Custom      map[string]any  `json:"custom,omitempty"`
	Event       json.RawMessage `json:"event"`
}

func makePayload(alert *pb.Alert, cfg Config) ([]byte, string, error) {
	if alert == nil {
		return nil, "", fmt.Errorf("alert is nil")
	}

	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return nil, "", fmt.Errorf("generate event ID: %w", err)
	}
	eventID := hex.EncodeToString(idBytes)

	rawEvent, err := json.Marshal(alert)
	if err != nil {
		return nil, "", fmt.Errorf("marshal raw alert: %w", err)
	}

	payload := APIEvent{
		EventID:     eventID,
		Source:      cfg.Source,
		ReceivedAt:  time.Now().UTC(),
		EventType:   "alert",
		Severity:    alert.GetSeverity(),
		Namespace:   alert.GetNamespaceName(),
		Pod:         alert.GetPodName(),
		Host:        alert.GetHostName(),
		Policy:      alert.GetPolicyName(),
		Operation:   alert.GetOperation(),
		Action:      alert.GetAction(),
		Result:      alert.GetResult(),
		Custom:      cfg.CustomFields,
		Event:       rawEvent,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, "", fmt.Errorf("marshal API payload: %w", err)
	}
	return body, eventID, nil
}
