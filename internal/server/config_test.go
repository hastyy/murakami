package server

import (
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCombineWith_EmptyConfigWithDefault(t *testing.T) {
	cfg := Config{}
	result := cfg.CombineWith(DefaultConfig)

	require.Equal(t, netip.AddrPort{}, result.Address)
	require.Nil(t, result.RWPool)

	require.NotNil(t, result.StartListener)
	require.NotNil(t, result.BackoffFunc)
	require.Equal(t, DefaultConfig.MaxAcceptDelay, result.MaxAcceptDelay)
}

func TestCombineWith_FullConfigWithDefault(t *testing.T) {
	var listenerCalled bool
	var backoffCalled bool

	cfg := Config{
		Address: netip.AddrPortFrom(netip.IPv4Unspecified(), 8080),
		RWPool:  &mockConnectionReadWriterPool{},
		StartListener: func(addr netip.AddrPort) (net.Listener, error) {
			listenerCalled = true
			return nil, nil
		},
		BackoffFunc: func(delay time.Duration) {
			backoffCalled = true
		},
		MaxAcceptDelay: 5 * time.Second,
	}

	result := cfg.CombineWith(DefaultConfig)

	require.Equal(t, netip.AddrPortFrom(netip.IPv4Unspecified(), 8080), result.Address)

	require.NotNil(t, result.RWPool)

	require.NotNil(t, result.StartListener)
	_, _ = result.StartListener(netip.AddrPortFrom(netip.IPv4Unspecified(), 8080))
	require.True(t, listenerCalled)

	require.NotNil(t, result.BackoffFunc)
	result.BackoffFunc(5 * time.Second)
	require.True(t, backoffCalled)

	require.Equal(t, 5*time.Second, result.MaxAcceptDelay)
	require.NotEqual(t, DefaultConfig.MaxAcceptDelay, result.MaxAcceptDelay)
}

func TestCombineWith_AddressIsNotCombined(t *testing.T) {
	// Test that Address field is not affected by CombineWith
	addr1 := netip.AddrPortFrom(netip.IPv4Unspecified(), 8080)
	addr2 := netip.AddrPortFrom(netip.IPv4Unspecified(), 9090)

	cfg := Config{
		Address:        addr1,
		StartListener:  nil,
		MaxAcceptDelay: 0,
	}

	other := Config{
		Address:        addr2,
		StartListener:  DefaultConfig.StartListener,
		MaxAcceptDelay: DefaultConfig.MaxAcceptDelay,
	}

	result := cfg.CombineWith(other)

	// Address should remain unchanged from the original config
	require.Equal(t, addr1, result.Address)
	// But other fields should be combined
	require.NotNil(t, result.StartListener)
	require.Equal(t, DefaultConfig.MaxAcceptDelay, result.MaxAcceptDelay)
}
