package mqredis

import (
	"context"
	"testing"
)

func TestMessageBind(t *testing.T) {
	msg, err := NewMessage(map[string]any{"name": "mqredis"})
	if err != nil {
		t.Fatalf("NewMessage() error = %v", err)
	}

	var payload struct {
		Name string `json:"name"`
	}
	if err := msg.Bind(&payload); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if payload.Name != "mqredis" {
		t.Fatalf("payload.Name = %q, want %q", payload.Name, "mqredis")
	}
}

func TestChainOrder(t *testing.T) {
	got := ""
	handler := chain(func(ctx context.Context, msg *Message) error {
		got += "handler"
		return nil
	}, func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) error {
			got += "a>"
			err := next(ctx, msg)
			got += "<a"
			return err
		}
	}, func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) error {
			got += "b>"
			err := next(ctx, msg)
			got += "<b"
			return err
		}
	})

	if err := handler(context.Background(), &Message{}); err != nil {
		t.Fatalf("handler() error = %v", err)
	}

	if got != "a>b>handler<b<a" {
		t.Fatalf("chain order = %q", got)
	}
}

func TestRecoverMiddleware(t *testing.T) {
	handler := Recover()(func(ctx context.Context, msg *Message) error {
		panic("boom")
	})

	err := handler(context.Background(), &Message{})
	if err == nil || err.Error() != "mqredis: panic recovered" {
		t.Fatalf("Recover() error = %v", err)
	}
}
