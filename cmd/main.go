package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hastyy/murakami/internal/bufferpool"
	"github.com/hastyy/murakami/internal/config"
	"github.com/hastyy/murakami/internal/handler"
	"github.com/hastyy/murakami/internal/protocol"
	"github.com/hastyy/murakami/internal/server"
	"github.com/hastyy/murakami/internal/service"
	"github.com/hastyy/murakami/internal/util"
)

func main() {
	// Create the base logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))

	// Parse the initial config
	cfg := parseConfig(logger)

	// Component initialization
	streamService := service.NewLogService(service.NewTimestampKeyGenerator())
	bufPool := bufferpool.New(bufferpool.Config{})
	commandDecoder := protocol.NewCommandDecoder(protocol.Config{
		BufPool: bufPool,
	})
	replyEncoder := protocol.NewReplyEncoder()
	handler := handler.NewHandler(streamService, commandDecoder, replyEncoder, logger)

	// Create a new server with the combined config and dependencies
	srv := server.New(server.Config{
		Address: cfg.Server.Address,
		// Create a new concurrent connection read writer pool with the given max concurrent connections as the size of the pool and connection read buffer size.
		RWPool: server.NewConcurrentConnectionReadWriterPool(cfg.Server.MaxConcurrentConnections, cfg.Server.ConnectionReadBufferSize),
	}.CombineWith(cfg.Server))

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
	// We don’t need to collect the server’s error if we didn’t already;
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

func parseConfig(logger *slog.Logger) config.Config {
	var addr string
	flag.StringVar(&addr, "server.addr", "localhost:7500", "Address in host:port format")

	var maxAcceptDelay time.Duration
	flag.DurationVar(&maxAcceptDelay, "server.maxAcceptDelay", 1*time.Second, "max delay when accepting new connections after error")

	var maxConcurrentConnections int
	flag.IntVar(&maxConcurrentConnections, "server.maxConcurrentConnections", 10_000, "maximum number of concurrent connections to accept")

	var connectionReadBufferSize int
	flag.IntVar(&connectionReadBufferSize, "server.connectionReadBufferSize", 1*util.KiB, "size of the buffer to use for reading from the connection")

	flag.Parse()

	addrPort, err := parseAddrPort(addr)
	if err != nil {
		logger.Error("unable to parse address + port", "error", err)
		os.Exit(1)
	}

	cfg := config.Config{
		Server: server.Config{
			Address:                  addrPort,
			MaxAcceptDelay:           maxAcceptDelay,
			MaxConcurrentConnections: maxConcurrentConnections,
			ConnectionReadBufferSize: connectionReadBufferSize,
		},
	}

	return cfg.CombineWith(config.DefaultConfig)
}

func parseAddrPort(addr string) (netip.AddrPort, error) {
	host, portStr, found := strings.Cut(addr, ":")
	if !found || portStr == "" {
		return netip.AddrPort{}, fmt.Errorf("invalid address format: %s", addr)
	}

	// Parse port
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("invalid port: %w", err)
	}

	// Handle empty host - default to unspecified address
	if host == "" {
		// Use IPv4 unspecified (0.0.0.0) or IPv6 (::)
		return netip.AddrPortFrom(netip.IPv4Unspecified(), uint16(port)), nil
	}

	if host == "localhost" {
		host = "127.0.0.1"
	}

	// Parse the IP address
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("invalid IP address: %w", err)
	}

	return netip.AddrPortFrom(ip, uint16(port)), nil
}
