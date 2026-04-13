package mqredis

import "context"

// Handler processes a message.
type Handler func(ctx context.Context, msg *Message) error

// Middleware wraps a handler.
type Middleware func(next Handler) Handler

func chain(h Handler, middlewares ...Middleware) Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}
