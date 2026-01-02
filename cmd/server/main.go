package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hastyy/murakami/internal/controller"
	"github.com/hastyy/murakami/internal/protocol"
	"github.com/hastyy/murakami/internal/store"
	"github.com/hastyy/murakami/internal/tcp"
)

func main() {
	// Create the base logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	keyGen := store.NewTimestampKeyGenerator()
	store := store.NewInMemoryStreamStore(keyGen)

	bufProvider := protocol.NewEagerAllocationBufferProvider()
	decoder := protocol.NewCommandDecoder(bufProvider)
	encoder := protocol.NewReplyEncoder()

	handler := controller.New(store, decoder, encoder, logger.With("component", "controller"))

	connPool := tcp.NewConnectionPool()
	listen := func(address string) (net.Listener, error) {
		return net.Listen("tcp", address)
	}
	srv := tcp.NewServer(connPool, listen, time.Sleep, tcp.ServerConfig{
		Address: ":7500",
	})

	// When the server stops it will return an error (can be nil) through this channel
	serr := make(chan error, 1)
	go func() { serr <- srv.Start(handler) }()

	// When this context is cancelled, we will try to gracefully stop the server
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	// Wait for either the server to fail, or the context to be cancelled
	var err error
	select {
	case err = <-serr:
	case <-ctx.Done():
	}

	// Make a best effort to shut down the server cleanly.
	// We don't need to collect the server's error if we didn't already;
	// Shutdown will let us know (unless something worse happens, in which case it will tell us that).
	sdctx, sdcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer sdcancel()
	if err = errors.Join(err, srv.Stop(sdctx)); err != nil {
		logger.Error("unable to perform graceful shutdown", "error", err)
		os.Exit(1)
	}
	logger.Info("server stopped gracefully")
	os.Exit(0)
}
