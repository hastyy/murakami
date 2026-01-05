package logging

import (
	"context"
	"log/slog"

	"github.com/hastyy/murakami/internal/tcp"
)

type contextKey string

const wideEventKey = contextKey("wideEvent")

type wideEvent struct {
	attrs []slog.Attr
}

func newWideEvent() *wideEvent {
	return &wideEvent{
		attrs: make([]slog.Attr, 0),
	}
}

func (event *wideEvent) Record(attr ...slog.Attr) {
	event.attrs = append(event.attrs, attr...)
}

func (event *wideEvent) Attrs() []slog.Attr {
	return event.attrs
}

func Middleware(logger *slog.Logger, h tcp.Handler) tcp.Handler {
	return tcp.HandlerFunc(func(ctx context.Context, c *tcp.Connection) error {
		event := newWideEvent()
		ctx = context.WithValue(ctx, wideEventKey, event)
		err := h.Handle(ctx, c)
		if err != nil {
			logger.LogAttrs(ctx, slog.LevelError, "error handling command", append(event.Attrs(), slog.Any("error", err))...)
		} else {
			logger.LogAttrs(ctx, slog.LevelInfo, "successful command", event.Attrs()...)
		}
		return err
	})
}

func Record(ctx context.Context, attr ...slog.Attr) {
	event, ok := ctx.Value(wideEventKey).(*wideEvent)
	if !ok {
		return
	}

	event.Record(attr...)
}
