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

type KeyGenerator interface {
	GetNext() logtree.Key
}

type stream struct {
	log     *logtree.LogTree[protocol.Record]
	watches []chan struct{}
	mu      sync.RWMutex
}

type InMemoryStreamStore struct {
	// KeyGenerator can be at the Store level (instead of having one per stream) because it's stateless.
	// If we were feeding it the last position of the stream, it would need to be a per-stream generator.
	keyGen  KeyGenerator
	streams map[string]*stream
	mu      sync.RWMutex
}

func NewInMemoryStreamStore(keyGen KeyGenerator) *InMemoryStreamStore {
	return &InMemoryStreamStore{
		keyGen:  keyGen,
		streams: make(map[string]*stream),
	}
}

func (s *InMemoryStreamStore) CreateStream(ctx context.Context, cmd protocol.CreateCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.streams[cmd.StreamName]; exists {
		return protocol.StreamExistsErrorf("stream %s already exists", cmd.StreamName)
	}

	s.streams[cmd.StreamName] = &stream{
		log: logtree.New[protocol.Record](),
	}

	return nil
}

func (s *InMemoryStreamStore) AppendRecords(ctx context.Context, cmd protocol.AppendCommand) (id string, err error) {
	assert.OK(cmd.Options.MillisID == "" || protocol.IsValidIDMillis(cmd.Options.MillisID), "id %s is not valid for append", cmd.Options.MillisID)

	s.mu.RLock()
	stream, exists := s.streams[cmd.StreamName]
	if !exists {
		s.mu.RUnlock()
		return "", protocol.UnknownStreamErrorf("stream %s not found", cmd.StreamName)
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	s.mu.RUnlock()

	// Type conversions between distinct string types (e.g., Key(string) and string(Key)) have zero runtime cost.
	// No copying occurs, just compile-time type reinterpretation of the same underlying {ptr, len} structure.
	key := logtree.Key(cmd.Options.MillisID)
	if key == "" {
		key = s.keyGen.GetNext()
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

func (s *InMemoryStreamStore) ReadRecords(ctx context.Context, cmd protocol.ReadCommand) ([]protocol.Record, error) {
	assert.OK(protocol.IsValidID(cmd.Options.MinID), "id %s is not valid for read", cmd.Options.MinID)
	assert.OK(cmd.Options.Block >= 0, "block must be greater than or equal to 0")
	assert.OK(cmd.Options.Count > 0, "count must be greater than 0")

	s.mu.RLock()
	stream, exists := s.streams[cmd.StreamName]
	if !exists {
		s.mu.RUnlock()
		return nil, protocol.UnknownStreamErrorf("stream %s not found", cmd.StreamName)
	}
	stream.mu.RLock()
	s.mu.RUnlock()

	parts := strings.Split(cmd.Options.MinID, "-")
	ms, seq := parts[0], parts[1]

	seqNum, err := strconv.Atoi(seq)
	assert.OK(err == nil, "invalid sequence number %s", seq)

	key := logtree.Key(ms)
	keyWithPadding := addLeftPadding(key)

	records := stream.log.Read(keyWithPadding, seqNum, cmd.Options.Count)
	if cmd.Options.Block == 0 || len(records) == cmd.Options.Count {
		stream.mu.RUnlock()
		return records, nil
	}

	stream.mu.RUnlock()

	startTime := time.Now()
	deadline := startTime.Add(cmd.Options.Block)
blockLoop:
	for len(records) < cmd.Options.Count {
		watch := make(chan struct{})

		s.mu.RLock()
		stream, exists := s.streams[cmd.StreamName]
		if !exists {
			s.mu.RUnlock()
			return records, nil
		}
		stream.mu.Lock()
		s.mu.RUnlock()

		stream.watches = append(stream.watches, watch)
		stream.mu.Unlock()

		select {
		case <-watch:
			s.mu.RLock()
			stream, exists := s.streams[cmd.StreamName]
			if !exists {
				s.mu.RUnlock()
				return records, nil
			}
			stream.mu.RLock()
			s.mu.RUnlock()

			currLastRecord := records[len(records)-1]
			parts := strings.Split(currLastRecord.ID, "-")
			ms, seq := parts[0], parts[1]

			seqNum, err := strconv.Atoi(seq)
			assert.OK(err == nil, "invalid sequence number %s", seq)

			key := logtree.Key(ms)
			keyWithPadding := addLeftPadding(key)

			records = append(records, stream.log.Read(keyWithPadding, seqNum, cmd.Options.Count-len(records))...)
			stream.mu.RUnlock()
		case <-time.After(time.Until(deadline)):
			break blockLoop
		}
	}

	return records, nil
}

func (s *InMemoryStreamStore) TrimStream(ctx context.Context, cmd protocol.TrimCommand) error {
	assert.OK(protocol.IsValidID(cmd.Options.MinID), "id %s is not valid for trim", cmd.Options.MinID)

	s.mu.RLock()
	stream, exists := s.streams[cmd.StreamName]
	if !exists {
		s.mu.RUnlock()
		return protocol.UnknownStreamErrorf("stream %s not found", cmd.StreamName)
	}
	stream.mu.Lock()
	s.mu.RUnlock()

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

func (s *InMemoryStreamStore) DeleteStream(ctx context.Context, cmd protocol.DeleteCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, exists := s.streams[cmd.StreamName]
	if !exists {
		return protocol.UnknownStreamErrorf("stream %s not found", cmd.StreamName)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()

	delete(s.streams, cmd.StreamName)

	for _, watch := range stream.watches {
		close(watch)
	}
	stream.watches = nil

	return nil
}

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
