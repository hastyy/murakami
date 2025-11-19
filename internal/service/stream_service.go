package service

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/hastyy/murakami/internal/logtree"
	"github.com/hastyy/murakami/internal/record"
)

var (
	ErrStreamExists                        = errors.New("stream exists")
	ErrUnknownStream                       = errors.New("stream not found")
	ErrBadFormatConflictingTimestampPolicy = errors.New("bad format: conflicting timestamp policy")
	ErrBadFormatNonMonotonicTimestamp      = errors.New("bad format: non-monotonic timestamp")
	ErrLimits                              = errors.New("limits exceeded")
)

type TimestampStrategy string

const (
	TimestampStrategyServer TimestampStrategy = "server"
	TimestampStrategyClient TimestampStrategy = "client"
)

type CreateRequest struct {
	StreamName string
	Strategy   TimestampStrategy
}

type AppendRequest struct {
	StreamName string
	Timestamp  int64
	Records    [][]byte
}

type ReadRequest struct {
	StreamName   string
	Count        int
	Block        time.Duration
	MinTimestamp record.Timestamp
}

type TrimRequest struct {
	StreamName string
	Until      record.Timestamp
}

type DeleteRequest struct {
	StreamName string
}

type Stream struct {
	Strategy TimestampStrategy
	Watches  []chan struct{}
	Log      *logtree.LogTree[record.Record]
}

type LogKeyGenerator interface {
	GetNext() logtree.Key
}

type StreamService struct {
	keyGenerator LogKeyGenerator

	mu      sync.RWMutex
	streams map[string]*Stream
}

func NewLogService(keyGenerator LogKeyGenerator) *StreamService {
	return &StreamService{
		keyGenerator: keyGenerator,
		streams:      make(map[string]*Stream),
	}
}

func (s *StreamService) CreateStream(ctx context.Context, req CreateRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.streams[req.StreamName]; ok {
		return ErrStreamExists
	}
	s.streams[req.StreamName] = &Stream{
		Strategy: req.Strategy,
		Log:      logtree.New[record.Record](),
	}

	return nil
}

func (s *StreamService) AppendRecords(ctx context.Context, req AppendRequest) (lastTimestamp record.Timestamp, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stream, ok := s.streams[req.StreamName]
	if !ok {
		return record.Timestamp{}, ErrUnknownStream
	}

	if stream.Strategy == TimestampStrategyServer && req.Timestamp != 0 || stream.Strategy == TimestampStrategyClient && req.Timestamp == 0 {
		return record.Timestamp{}, ErrBadFormatConflictingTimestampPolicy
	}

	var key logtree.Key
	if stream.Strategy == TimestampStrategyServer {
		key = s.keyGenerator.GetNext()
	} else {
		key = logtree.Key(strconv.FormatInt(req.Timestamp, 10))
	}

	lastKey, lastSeqNum := stream.Log.LastPosition()
	var seqNum int
	if lastKey == key {
		seqNum = lastSeqNum + 1
	}

	records := make([]record.Record, 0, len(req.Records))
	for _, recordBytes := range req.Records {
		records = append(records, record.Record{
			Timestamp: record.Timestamp{
				Millis: req.Timestamp,
				SeqNum: seqNum,
			},
			Value: recordBytes,
		})
		seqNum++
	}

	stream.Log.Append(key, records)

	for _, watch := range stream.Watches {
		close(watch)
	}
	stream.Watches = nil

	lastKey, lastSeqNum = stream.Log.LastPosition()
	millis, err := strconv.ParseInt(string(lastKey), 10, 64)
	if err != nil {
		return record.Timestamp{}, err
	}

	return record.Timestamp{
		Millis: millis,
		SeqNum: lastSeqNum,
	}, nil
}

func (s *StreamService) ReadRecords(ctx context.Context, req ReadRequest) (records []record.Record, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stream, ok := s.streams[req.StreamName]
	if !ok {
		return nil, ErrUnknownStream
	}

	key := logtree.Key(strconv.FormatInt(req.MinTimestamp.Millis, 10))
	records = stream.Log.Read(key, req.MinTimestamp.SeqNum, req.Count)

	currTime := time.Now()
	deadline := currTime.Add(req.Block)
loop:
	for len(records) < req.Count && currTime.Before(deadline) {
		watch := make(chan struct{})
		stream.Watches = append(stream.Watches, watch)
		s.mu.RUnlock()

		select {
		case <-watch:
			s.mu.RLock()
			stream, ok = s.streams[req.StreamName]
			if !ok {
				records = nil
				err = ErrUnknownStream
				break loop
			}

			from := logtree.Key(strconv.FormatInt(records[len(records)-1].Timestamp.Millis, 10))
			seqNum := records[len(records)-1].Timestamp.SeqNum
			limit := req.Count - len(records)
			records = append(records, stream.Log.Read(from, seqNum, limit)...)

			currTime = time.Now()
		case <-time.After(deadline.Sub(currTime)):
			s.mu.RLock()
			break loop
		}
	}

	return records, err
}

func (s *StreamService) TrimStream(ctx context.Context, req TrimRequest) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stream, ok := s.streams[req.StreamName]
	if !ok {
		return ErrUnknownStream
	}

	key := logtree.Key(strconv.FormatInt(req.Until.Millis, 10))
	stream.Log.Trim(key, req.Until.SeqNum)

	return nil
}

func (s *StreamService) DeleteStream(ctx context.Context, req DeleteRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.streams[req.StreamName]
	if !ok {
		return ErrUnknownStream
	}

	delete(s.streams, req.StreamName)

	return nil
}
