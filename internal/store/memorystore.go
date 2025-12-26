package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hastyy/murakami/internal/assert"
	"github.com/hastyy/murakami/internal/protocol"
	"github.com/hastyy/murakami/internal/store/logtree"
)

// KeyGenerator generates unique, monotonically increasing keys for stream records.
// Implementations must be safe for concurrent access as the same generator may be used
// across multiple goroutines appending to different streams.
type KeyGenerator interface {
	// GetNext returns the next key to use for a record.
	// Keys should be monotonically increasing to maintain proper record ordering.
	GetNext() logtree.Key
}

// stream represents a single stream with its log and active watches.
// watches holds channels for blocked readers waiting for new records.
// The mutex protects concurrent access to both the log and watches slice.
type stream struct {
	log     *logtree.LogTree[protocol.Record]
	watches []chan struct{}
	mu      sync.RWMutex
}

// InMemoryStreamStore is a thread-safe, in-memory implementation of the StreamStore interface.
// It uses a LogTree for efficient record storage and supports blocking reads via a watch mechanism.
// All public methods are safe for concurrent access. The KeyGenerator is shared across all streams
// and is used when clients don't provide explicit millisecond IDs during append operations.
type InMemoryStreamStore struct {
	// KeyGenerator can be at the Store level (instead of having one per stream) because it's stateless.
	// If we were feeding it the last position of the stream, it would need to be a per-stream generator.
	keyGen  KeyGenerator
	streams map[string]*stream
	mu      sync.RWMutex
}

// NewInMemoryStreamStore creates a new InMemoryStreamStore with the given key generator.
func NewInMemoryStreamStore(keyGen KeyGenerator) *InMemoryStreamStore {
	return &InMemoryStreamStore{
		keyGen:  keyGen,
		streams: make(map[string]*stream),
	}
}

// CreateStream creates a new stream with the given name.
// Returns protocol.Error with ErrCodeStreamExists if a stream with this name already exists.
// The stream is initialized with an empty log and no active watches.
func (store *InMemoryStreamStore) CreateStream(ctx context.Context, cmd protocol.CreateCommand) error {
	assert.OK(len(cmd.StreamName) > 0, "stream name can't be empty")

	store.mu.Lock()
	defer store.mu.Unlock()

	if _, exists := store.streams[cmd.StreamName]; exists {
		return protocol.StreamExistsErrorf("stream %s already exists", cmd.StreamName)
	}

	store.streams[cmd.StreamName] = &stream{
		log: logtree.New[protocol.Record](),
	}

	return nil
}

// AppendRecords appends one or more records to an existing stream.
// If cmd.Options.MillisID is provided, it's used as the millisecond component of generated IDs;
// otherwise, the KeyGenerator provides the timestamp. Sequence numbers are auto-incremented within
// the same millisecond to ensure strict monotonicity. All active watches are closed and notified
// upon successful append. Returns the ID of the last appended record.
// Returns protocol.Error with ErrCodeUnknownStream if the stream doesn't exist.
// Returns protocol.Error with ErrCodeNonMonotonicID if the provided MillisID is less than the last ID.
func (store *InMemoryStreamStore) AppendRecords(ctx context.Context, cmd protocol.AppendCommand) (id string, err error) {
	assert.OK(len(cmd.StreamName) > 0, "stream name can't be empty")
	assert.OK(len(cmd.Records) > 0, "at least one record is required")
	assert.OK(cmd.Options.MillisID == "" || protocol.IsValidIDMillis(cmd.Options.MillisID), "id %s is not valid for append", cmd.Options.MillisID)

	store.mu.RLock()
	stream, exists := store.streams[cmd.StreamName]
	if !exists {
		store.mu.RUnlock()
		return "", protocol.UnknownStreamErrorf("stream %s not found", cmd.StreamName)
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	store.mu.RUnlock()

	// Type conversions between distinct string types (e.g., Key(string) and string(Key)) have zero runtime cost.
	// No copying occurs, just compile-time type reinterpretation of the same underlying {ptr, len} structure.
	key := logtree.Key(cmd.Options.MillisID)
	if key == "" {
		key = store.keyGen.GetNext()
	}
	keyWithPadding := addLeftPadding(key)

	lastKey, lastSeqNum := stream.log.LastPosition()
	if keyWithPadding < lastKey {
		return "", protocol.NonMonotonicIDErrorf("id %s is less than the last id %s", key, lastKey)
	}

	var nextSeqNum int
	if keyWithPadding == lastKey {
		nextSeqNum = lastSeqNum + 1
	}

	records := make([]protocol.Record, 0, len(cmd.Records))
	for i, record := range cmd.Records {
		records = append(records, protocol.Record{
			ID:    fmt.Sprintf("%s-%d", key, nextSeqNum+i),
			Value: record,
		})
	}
	stream.log.Append(keyWithPadding, records)

	for _, watch := range stream.watches {
		close(watch)
	}
	stream.watches = nil

	return records[len(records)-1].ID, nil
}

// ReadRecords reads records from a stream starting from cmd.Options.MinID (inclusive).
// Returns up to cmd.Options.Count records. If cmd.Options.Block is non-zero and no records are
// immediately available, registers a watch and blocks for up to that duration waiting for new records.
// The read is canceled early if ctx is canceled, returning ctx.Err(). If the stream is deleted while
// blocking, returns successfully with whatever records were read (possibly empty).
// Returns an empty slice if no records match the criteria (not an error).
// Returns protocol.Error with ErrCodeUnknownStream if the stream doesn't exist at the start of the operation.
func (store *InMemoryStreamStore) ReadRecords(ctx context.Context, cmd protocol.ReadCommand) ([]protocol.Record, error) {
	assert.OK(protocol.IsValidID(cmd.Options.MinID), "id %s is not valid for read", cmd.Options.MinID)
	assert.OK(cmd.Options.Block >= 0, "block must be greater than or equal to 0")
	assert.OK(cmd.Options.Count > 0, "count must be greater than 0")

	store.mu.RLock()
	stream, exists := store.streams[cmd.StreamName]
	if !exists {
		store.mu.RUnlock()
		return nil, protocol.UnknownStreamErrorf("stream %s not found", cmd.StreamName)
	}
	stream.mu.RLock()
	store.mu.RUnlock()

	idParts := strings.Split(cmd.Options.MinID, "-")
	ms, seq := idParts[0], idParts[1]

	seqNum, err := strconv.Atoi(seq)
	assert.OK(err == nil, "invalid sequence number %s", seq)

	key := logtree.Key(ms)
	keyWithPadding := addLeftPadding(key)

	records := stream.log.Read(keyWithPadding, seqNum, cmd.Options.Count)
	if cmd.Options.Block == 0 || len(records) > 0 {
		stream.mu.RUnlock()
		return records, nil
	}

	stream.mu.RUnlock()

	startTime := time.Now()
	deadline := startTime.Add(cmd.Options.Block)

	watch := make(chan struct{})

	// We need to find the stream again because it might have been deleted since we released the read lock.
	store.mu.RLock()
	stream, exists = store.streams[cmd.StreamName]
	if !exists {
		store.mu.RUnlock()
		return records, nil
	}
	stream.mu.Lock()
	store.mu.RUnlock()

	// We need to do a fresh read here because an append might have happened since we released the read lock.
	records = stream.log.Read(keyWithPadding, seqNum, cmd.Options.Count)
	if len(records) > 0 {
		stream.mu.Unlock()
		close(watch)
		return records, nil
	}

	// Still no records, so we need to wait for an append to happen.
	stream.watches = append(stream.watches, watch)
	stream.mu.Unlock()

	select {
	case <-watch:
		// We need to find the stream again because the watch might have been closed by a delete stream operation.
		store.mu.RLock()
		stream, exists := store.streams[cmd.StreamName]
		if !exists {
			store.mu.RUnlock()
			return records, nil
		}
		stream.mu.RLock()
		store.mu.RUnlock()

		// If we reach here, an append must have happened since we released the read lock.
		// So we need to do a fresh read and return the results.
		//
		// This will only return 0 records if a trim operation has happened since we released the read lock.
		// Although that is very unlikely to happen (means the records were trimmed right after being added),
		// it would not be a problem since we would just return empty anyway.
		records = stream.log.Read(keyWithPadding, seqNum, cmd.Options.Count)
		stream.mu.RUnlock()
	case <-time.After(time.Until(deadline)):
		break
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return records, nil
}

// TrimStream removes all records with IDs strictly less than cmd.Options.MinID from the stream.
// Records at or after MinID are preserved. This is a permanent deletion operation used to reclaim
// storage space. Trimming an empty stream is a no-op and succeeds.
// Returns protocol.Error with ErrCodeUnknownStream if the stream doesn't exist.
func (store *InMemoryStreamStore) TrimStream(ctx context.Context, cmd protocol.TrimCommand) error {
	assert.OK(protocol.IsValidID(cmd.Options.MinID), "id %s is not valid for trim", cmd.Options.MinID)

	store.mu.RLock()
	stream, exists := store.streams[cmd.StreamName]
	if !exists {
		store.mu.RUnlock()
		return protocol.UnknownStreamErrorf("stream %s not found", cmd.StreamName)
	}
	stream.mu.Lock()
	store.mu.RUnlock()

	parts := strings.Split(cmd.Options.MinID, "-")
	ms, seq := parts[0], parts[1]

	seqNum, err := strconv.Atoi(seq)
	assert.OK(err == nil, "invalid sequence number %s", seq)

	key := logtree.Key(ms)
	keyWithPadding := addLeftPadding(key)

	stream.log.Trim(keyWithPadding, seqNum)
	stream.mu.Unlock()

	return nil
}

// DeleteStream permanently deletes a stream and all its records.
// All active watches are closed and notified before the stream is removed, allowing blocked readers
// to complete gracefully. After deletion, subsequent operations on this stream will return ErrCodeUnknownStream.
// Returns protocol.Error with ErrCodeUnknownStream if the stream doesn't exist.
func (store *InMemoryStreamStore) DeleteStream(ctx context.Context, cmd protocol.DeleteCommand) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	stream, exists := store.streams[cmd.StreamName]
	if !exists {
		return protocol.UnknownStreamErrorf("stream %s not found", cmd.StreamName)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()

	delete(store.streams, cmd.StreamName)

	for _, watch := range stream.watches {
		close(watch)
	}
	stream.watches = nil

	return nil
}

// addLeftPadding pads a key with leading zeros to ensure it's exactly 20 characters.
// This normalization allows for correct lexicographic comparison of keys in the LogTree,
// as "10000" would otherwise sort before "9999" lexicographically despite being numerically greater.
// Expects keys with length <= 20.
func addLeftPadding(key logtree.Key) logtree.Key {
	assert.OK(len(key) <= 20, "key length must be less than or equal to 20")
	if len(key) == 20 {
		return key
	}

	return logtree.Key(strings.Repeat("0", 20-len(key)) + string(key))
}

// func removeLeftPadding(key logtree.Key) logtree.Key {
// 	for i := 0; i < len(key); i++ {
// 		if key[i] != '0' {
// 			return key[i:]
// 		}
// 	}
// 	return ""
// }
