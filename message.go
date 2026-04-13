package mqredis

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

var messageSeq uint64

// Message is the transport envelope stored in Redis Streams.
type Message struct {
	ID          string            `json:"id"`
	Topic       string            `json:"topic"`
	Payload     json.RawMessage   `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	Timestamp   int64             `json:"timestamp"`
	Retry       int               `json:"retry"`
	AvailableAt int64             `json:"available_at,omitempty"`
}

// NewMessage builds a serializable message from any payload.
func NewMessage(payload any) (*Message, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Message{
		ID:        newMessageID(),
		Payload:   raw,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// Bind decodes the payload into a strongly typed value.
func (m *Message) Bind(v any) error {
	if len(m.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(m.Payload, v)
}

// Clone creates a deep copy so middleware and handlers can modify safely.
func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}

	clone := *m
	if len(m.Payload) > 0 {
		clone.Payload = append(json.RawMessage(nil), m.Payload...)
	}
	if len(m.Headers) > 0 {
		clone.Headers = make(map[string]string, len(m.Headers))
		for k, v := range m.Headers {
			clone.Headers[k] = v
		}
	}
	return &clone
}

func (m *Message) ensureDefaults(topic string) {
	if m.ID == "" {
		m.ID = newMessageID()
	}
	if m.Topic == "" {
		m.Topic = topic
	}
	if m.Timestamp == 0 {
		m.Timestamp = time.Now().UnixMilli()
	}
}

func newMessageID() string {
	seq := atomic.AddUint64(&messageSeq, 1)
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), seq)
}
