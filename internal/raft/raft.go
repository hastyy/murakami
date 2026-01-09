package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hastyy/murakami/internal/assert"
)

var (
	ErrAlreadyStarted                  = errors.New("raft instance already started")
	ErrAlreadyStopped                  = errors.New("raft instance already stopped")
	ErrNotStarted                      = errors.New("raft instance not started")
	ErrNotLeader                       = errors.New("raft instance not leader")
	ErrPersistedStateNotFound          = errors.New("persisted state not found")
	ErrPersistedLogNotFound            = errors.New("persisted log not found")
	ErrMaxPendingAppendRequestsReached = errors.New("max pending append requests reached")
)

var DefaultConfig = Config{
	MinElectionTimeout:       150 * time.Millisecond,
	MaxElectionTimeout:       300 * time.Millisecond,
	LeaderHeartbeatInterval:  50 * time.Millisecond,
	MaxPendingAppendRequests: 10_000,
}

// NodeID is a unique identifier for each node in the cluster.
// This can represent up to 65_535 (or (2^16)-1) nodes which should
// be more than enough for most deployments today.
type NodeID uint16

// NodeID of value 0 is reserved to represent the absence of a node.
const ZeroNodeID NodeID = 0

type Config struct {
	// Instance params (passed once through the constructor)
	ID    NodeID
	Peers []NodeID

	// Instance Dependencies (passed once through the constructor)
	FiniteStateMachine    FiniteStateMachine
	StateStore            StateStore
	LogStore              LogStore
	Network               Network
	RandomNumberGenerator RandomNumberGenerator
	ElectionTimer         Timer
	LeaderHeartbeatTicker Ticker
	Slog                  *slog.Logger

	// Config values (referenced throughout the code to configure timeouts, etc.)
	MinElectionTimeout       time.Duration
	MaxElectionTimeout       time.Duration
	LeaderHeartbeatInterval  time.Duration
	MaxPendingAppendRequests int
}

func (cfg Config) Mixin(other Config) Config {
	if other.ID != ZeroNodeID {
		cfg.ID = other.ID
	}
	if len(other.Peers) > 0 {
		cfg.Peers = other.Peers
	}
	if other.FiniteStateMachine != nil {
		cfg.FiniteStateMachine = other.FiniteStateMachine
	}
	if other.StateStore != nil {
		cfg.StateStore = other.StateStore
	}
	if other.LogStore != nil {
		cfg.LogStore = other.LogStore
	}
	if other.Network != nil {
		cfg.Network = other.Network
	}
	if other.RandomNumberGenerator != nil {
		cfg.RandomNumberGenerator = other.RandomNumberGenerator
	}
	if other.ElectionTimer != nil {
		cfg.ElectionTimer = other.ElectionTimer
	}
	if other.LeaderHeartbeatTicker != nil {
		cfg.LeaderHeartbeatTicker = other.LeaderHeartbeatTicker
	}
	if other.Slog != nil {
		cfg.Slog = other.Slog
	}
	if other.MinElectionTimeout > 0 {
		cfg.MinElectionTimeout = other.MinElectionTimeout
	}
	if other.MaxElectionTimeout > 0 {
		cfg.MaxElectionTimeout = other.MaxElectionTimeout
	}
	if other.LeaderHeartbeatInterval > 0 {
		cfg.LeaderHeartbeatInterval = other.LeaderHeartbeatInterval
	}
	if other.MaxPendingAppendRequests > 0 {
		cfg.MaxPendingAppendRequests = other.MaxPendingAppendRequests
	}
	return cfg
}

type role byte

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	switch r {
	case follower:
		return "Follower"
	case candidate:
		return "Candidate"
	case leader:
		return "Leader"
	default:
		panic("invalid role")
	}
}

type LogEntry struct {
	Term uint64
	Cmd  []byte
}

type RandomNumberGenerator interface {
	GenerateDurationInRange(min, max time.Duration) time.Duration
}

type Timer interface {
	Timeout() <-chan time.Time
	Reset(time.Duration)
	Stop()
	Close()
}

type Ticker interface {
	Tick() <-chan time.Time
	Reset(time.Duration)
	Stop()
	Close()
}

type Network interface {
	SendVoteRequest(peer NodeID, req VoteRequest)
	SendVoteResponse(candidate NodeID, res VoteResponse)
	SendAppendEntriesRequest(follower NodeID, req AppendEntriesRequest)
	SendAppendEntriesResponse(leader NodeID, res AppendEntriesResponse)
	Message() <-chan Message
}

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    NodeID
}

type StateStore interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

type LogStore interface {
	Load() ([]LogEntry, error)
	Append([]LogEntry) error
}

type CommittedEntry struct {
	LogSeqNum int
	Cmd       []byte
}

type FiniteStateMachine interface {
	Apply(CommittedEntry) error
}

type Message interface {
	Handle(raft *Instance)
}

// Ensure that the messages implement the Message interface
var _ Message = &VoteRequest{}
var _ Message = &VoteResponse{}
var _ Message = &AppendEntriesRequest{}
var _ Message = &AppendEntriesResponse{}

type VoteRequest struct {
	CandidateID          NodeID
	CandidateTerm        uint64
	CandidateLogLength   int
	CandidateLastLogTerm uint64
}

type VoteResponse struct {
	VoterID     NodeID
	VoterTerm   uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	LeaderID           NodeID
	LeaderTerm         uint64
	LeaderCommitLength int
	PrefixLength       int
	PrefixTerm         uint64
	Suffix             []LogEntry
}

type AppendEntriesResponse struct {
	FollowerID   NodeID
	FollowerTerm uint64
	Ack          int
	Success      bool
}

type appendRequest struct {
	cmd      []byte
	resultCh chan error
}

type instanceState int

const (
	idle instanceState = iota
	running
	stopped
)

type Instance struct {
	cfg Config

	id    NodeID
	peers []NodeID

	// Volatile state on all servers
	role role

	// Volatile state on candidates (re-initialised when starting election)
	votesReceived Set[NodeID]

	// Volatile state on leaders (re-initialised after winning election)
	sentLength  map[NodeID]int
	ackedLength map[NodeID]int
	purgatory   map[int]appendRequest

	currTerm     uint64
	votedFor     NodeID
	commitLength int
	log          []LogEntry

	// Dependencies
	fsm                   FiniteStateMachine
	stateStore            StateStore
	logStore              LogStore
	network               Network
	rng                   RandomNumberGenerator
	electionTimer         Timer
	leaderHeartbeatTicker Ticker
	slog                  *slog.Logger

	// Channel for incoming append requests
	requestQueue chan appendRequest

	// Instance synchronization
	runningState instanceState
	mu           sync.RWMutex
	stopCh       chan struct{}
}

func New(cfg Config) *Instance {
	cfg = DefaultConfig.Mixin(cfg)

	assert.NonZero(cfg.ID, "cfg.ID is required")
	assert.NonEmpty(cfg.Peers, "cfg.Peers is required")
	assert.NonNil(cfg.FiniteStateMachine, "cfg.FiniteStateMachine is required")
	assert.NonNil(cfg.StateStore, "cfg.StateStore is required")
	assert.NonNil(cfg.LogStore, "cfg.LogStore is required")
	assert.NonNil(cfg.Network, "cfg.Network is required")
	assert.NonNil(cfg.RandomNumberGenerator, "cfg.RandomNumberGenerator is required")
	assert.NonNil(cfg.ElectionTimer, "cfg.ElectionTimer is required")
	assert.NonNil(cfg.LeaderHeartbeatTicker, "cfg.LeaderHeartbeatTicker is required")
	assert.NonNil(cfg.Slog, "cfg.Slog is required")

	return &Instance{
		cfg:                   cfg,
		id:                    cfg.ID,
		peers:                 cfg.Peers,
		fsm:                   cfg.FiniteStateMachine,
		stateStore:            cfg.StateStore,
		logStore:              cfg.LogStore,
		network:               cfg.Network,
		rng:                   cfg.RandomNumberGenerator,
		electionTimer:         cfg.ElectionTimer,
		leaderHeartbeatTicker: cfg.LeaderHeartbeatTicker,
		slog:                  cfg.Slog,
		runningState:          idle,
		stopCh:                make(chan struct{}),
	}
}

func (raft *Instance) Start() error {
	raft.mu.Lock()

	switch raft.runningState {
	case stopped:
		raft.mu.Unlock()
		return ErrAlreadyStopped
	case running:
		raft.mu.Unlock()
		return ErrAlreadyStarted
	}

	raft.role = follower
	raft.votesReceived = NewSet[NodeID](len(raft.peers) + 1)

	// we pre-allocate the maps to avoid a new allocation each time we become leader
	raft.sentLength = make(map[NodeID]int, len(raft.peers))
	raft.ackedLength = make(map[NodeID]int, len(raft.peers))
	raft.purgatory = make(map[int]appendRequest, raft.cfg.MaxPendingAppendRequests)

	// Initial state
	raft.currTerm = 0
	raft.votedFor = ZeroNodeID

	// Initial log
	raft.commitLength = 0
	raft.log = nil

	if persistedState, err := raft.stateStore.Load(); err == nil {
		raft.slog.Debug("loaded persisted state", "persisted_state", persistedState)
		raft.currTerm = persistedState.CurrentTerm
		raft.votedFor = persistedState.VotedFor
	} else if !errors.Is(err, ErrPersistedStateNotFound) {
		raft.mu.Unlock()
		return fmt.Errorf("failed to load persisted state: %w", err)
	}

	if persistedLog, err := raft.logStore.Load(); err == nil {
		raft.slog.Debug("loaded persisted log", "persisted_log", persistedLog)
		raft.log = persistedLog
	} else if !errors.Is(err, ErrPersistedLogNotFound) {
		raft.mu.Unlock()
		return fmt.Errorf("failed to load persisted log: %w", err)
	}

	raft.restartElectionTimer()

	raft.runningState = running
	raft.mu.Unlock()

eventLoop:
	for {
		select {
		case <-raft.stopCh:
			break eventLoop
		case req := <-raft.requestQueue:
			raft.appendLogEntry(req)
		case msg := <-raft.network.Message():
			msg.Handle(raft)
		case <-raft.electionTimer.Timeout():
			raft.startElection()
		case <-raft.leaderHeartbeatTicker.Tick():
			raft.periodicReplication()
		}
	}

	return nil
}

func (raft *Instance) Stop(ctx context.Context) error {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	if raft.runningState == stopped {
		return ErrAlreadyStopped
	}

	isRunning := raft.runningState == running

	close(raft.stopCh)
	raft.runningState = stopped

	if !isRunning {
		return nil
	}

	for _, req := range raft.purgatory {
		select {
		case req.resultCh <- ErrAlreadyStopped:
		default:
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (raft *Instance) Append(ctx context.Context, cmd []byte) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	raft.mu.RLock()

	if raft.runningState == stopped {
		raft.mu.RUnlock()
		return ErrAlreadyStopped
	}

	if raft.runningState == idle {
		raft.mu.RUnlock()
		return ErrNotStarted
	}

	if raft.role != leader {
		raft.mu.RUnlock()
		return ErrNotLeader
	}

	raft.mu.RUnlock()

	req := appendRequest{
		cmd:      cmd,
		resultCh: make(chan error, 1),
	}

	// Put the request in the queue
	select {
	case <-ctx.Done():
		return ctx.Err()
	case raft.requestQueue <- req:
	}

	// Wait for the result
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-req.resultCh:
		return err
	}
}

func (raft *Instance) restartElectionTimer() {
	min := raft.cfg.MinElectionTimeout
	max := raft.cfg.MaxElectionTimeout
	randomTimerDuration := raft.rng.GenerateDurationInRange(min, max)

	raft.slog.Debug("restarting election timer", "duration", randomTimerDuration)
	raft.electionTimer.Reset(randomTimerDuration)
}

func (raft *Instance) startElection() {
	assert.OK(raft.role == follower || raft.role == candidate, "attempted to start election while not a follower or candidate")
	raft.slog.Debug("starting new election for term", "term", raft.currTerm+1)

	raft.votesReceived.Clear()
	raft.votesReceived.Add(raft.id)

	raft.role = candidate
	raft.votedFor = raft.id
	raft.currTerm++

	raft.persistState()

	var lastTerm uint64
	if len(raft.log) > 0 {
		lastTerm = raft.log[len(raft.log)-1].Term
	}

	req := VoteRequest{
		CandidateID:          raft.id,
		CandidateTerm:        raft.currTerm,
		CandidateLogLength:   len(raft.log),
		CandidateLastLogTerm: lastTerm,
	}

	raft.slog.Debug("broadcasting vote request", "term", raft.currTerm, "log_length", len(raft.log), "last_log_term", lastTerm)
	for _, peer := range raft.peers {
		raft.network.SendVoteRequest(peer, req)
	}

	raft.restartElectionTimer()
}

func (req VoteRequest) Handle(raft *Instance) {
	assert.NonZero(req.CandidateID, "req.CandidateID is required")
	raft.slog.Debug("received vote request",
		"candidate_id", req.CandidateID,
		"candidate_term", req.CandidateTerm,
		"candidate_log_length", req.CandidateLogLength,
		"candidate_last_log_term", req.CandidateLastLogTerm)

	var shouldRestartElectionTimer bool
	defer func() {
		if shouldRestartElectionTimer {
			raft.restartElectionTimer()
		}
	}()

	if req.CandidateTerm > raft.currTerm {
		raft.slog.Debug("candidate term is higher, transitioning to follower")
		raft.transitionToFollower(req.CandidateTerm)

		shouldRestartElectionTimer = true
	}

	var lastTerm uint64
	if len(raft.log) > 0 {
		lastTerm = raft.log[len(raft.log)-1].Term
	}

	isCandidateLogOk := req.CandidateLastLogTerm > lastTerm || (req.CandidateLastLogTerm == lastTerm && req.CandidateLogLength >= len(raft.log))
	hasVotedForCandidateOrNone := raft.votedFor == ZeroNodeID || raft.votedFor == req.CandidateID

	if req.CandidateTerm == raft.currTerm && isCandidateLogOk && hasVotedForCandidateOrNone {
		raft.votedFor = req.CandidateID
		raft.persistState()

		res := VoteResponse{
			VoterID:     raft.id,
			VoterTerm:   raft.currTerm,
			VoteGranted: true,
		}

		raft.slog.Debug("sending successful vote response", "candidate_id", req.CandidateID, "term", raft.currTerm)
		raft.network.SendVoteResponse(req.CandidateID, res)

		shouldRestartElectionTimer = true
	} else {
		res := VoteResponse{
			VoterID:     raft.id,
			VoterTerm:   raft.currTerm,
			VoteGranted: false,
		}

		raft.slog.Debug("sending rejected vote response",
			"candidate_id", req.CandidateID,
			"term", raft.currTerm,
			"candidate_term", req.CandidateTerm,
			"is_candidate_log_ok", isCandidateLogOk,
			"has_voted_for_candidate_or_none", hasVotedForCandidateOrNone)
		raft.network.SendVoteResponse(req.CandidateID, res)
	}
}

func (res VoteResponse) Handle(raft *Instance) {
	raft.slog.Debug("received vote response",
		"voter_id", res.VoterID,
		"voter_term", res.VoterTerm,
		"vote_granted", res.VoteGranted)

	if res.VoterTerm > raft.currTerm {
		raft.slog.Debug("voter term is higher, transitioning to follower", "current_term", raft.currTerm, "voter_term", res.VoterTerm)

		raft.transitionToFollower(res.VoterTerm)

		raft.restartElectionTimer()
	} else if res.VoterTerm == raft.currTerm && raft.role == candidate && res.VoteGranted {
		raft.votesReceived.Add(res.VoterID)

		if raft.hasVotingMajority() {
			assert.OK(raft.role == candidate, "attempting to transition to leader while not a candidate: current_role = %d", raft.role)
			assert.OK(raft.votedFor == raft.id, "becoming leader but didn't vote for self: voted_for = %d", raft.votedFor)
			raft.slog.Debug("received majority of votes, transitioning to leader", "term", raft.currTerm)

			raft.role = leader
			raft.electionTimer.Stop()

			for _, follower := range raft.peers {
				raft.sentLength[follower] = len(raft.log)
				raft.ackedLength[follower] = 0

				// Immediately heartbeat once we become leader.
				raft.replicateLog(follower)
			}

			raft.leaderHeartbeatTicker.Reset(raft.cfg.LeaderHeartbeatInterval)
		}
	}
}

func (req AppendEntriesRequest) Handle(raft *Instance) {
	raft.slog.Debug("received append entries request",
		"leader_id", req.LeaderID,
		"leader_term", req.LeaderTerm,
		"leader_commit_length", req.LeaderCommitLength,
		"prefix_length", req.PrefixLength,
		"prefix_term", req.PrefixTerm,
		"suffix_length", len(req.Suffix))

	var shouldRestartElectionTimer bool
	defer func() {
		if shouldRestartElectionTimer {
			raft.restartElectionTimer()
		}
	}()

	// If we enter this condition then we'll also enter the next if block as a fallthrough condition
	// since we set raft.currTerm = req.LeaderTerm and thus afterwards raft.currTerm == req.LeaderTerm
	if req.LeaderTerm > raft.currTerm {
		raft.slog.Debug("leader term is higher, transitioning to follower", "current_term", raft.currTerm, "leader_term", req.LeaderTerm)

		raft.transitionToFollower(req.LeaderTerm)
	}

	if req.LeaderTerm == raft.currTerm {
		raft.role = follower
		shouldRestartElectionTimer = true
	}

	// Checks that our log is at least as up-to-date as the leader thinks it is.
	// The prefix is how much of the log the leader believes we have.
	// The suffix is how much of the log the leader believes we don't have.
	// This check is two-fold:
	// 	1. The length of our log is at least as long as the prefix length (len(raft.log) >= req.PrefixLength)
	// 	2. The prefix term (aka the term of the last entry in the prefix) is equal to the term of the same entry in our log (raft.log[req.PrefixLength-1].Term == req.PrefixTerm)
	// In 2. there's also the case where req.PrefixLength == 0, in which case we don't have any prefix and thus the term check is not needed.
	// If this evaluates to true then we know that our log is at least as big and identical to the assumed prefix.
	// And if the log is bigger (i.e. len(raft.log) > req.PrefixLength), then we know that it is identical to the prefix up to raft.log[req.PrefixLength-1].
	// This is guaranteed by the Log Matching invariant of Raft which states:
	// 	- If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index.
	//
	// So if ourLogContainsPrefix then raft.log == [...prefix, ...maybe_some_other_entries]
	// otherwise, either len(raft.log) < req.PrefixLength or it contains different entries than the prefix for the same index(es).
	ourLogContainsPrefix := len(raft.log) >= req.PrefixLength && (req.PrefixLength == 0 || raft.log[req.PrefixLength-1].Term == req.PrefixTerm)

	if req.LeaderTerm == raft.currTerm && ourLogContainsPrefix {
		raft.slog.Debug("appending new entries from leader",
			"leader_id", req.LeaderID,
			"leader_term", req.LeaderTerm,
			"prefix_length", req.PrefixLength,
			"suffix_length", len(req.Suffix))

		raft.appendEntries(req.PrefixLength, req.LeaderCommitLength, req.Suffix)
		ack := req.PrefixLength + len(req.Suffix)

		res := AppendEntriesResponse{
			FollowerID:   raft.id,
			FollowerTerm: raft.currTerm,
			Ack:          ack,
			Success:      true,
		}

		raft.network.SendAppendEntriesResponse(req.LeaderID, res)
	} else {
		raft.slog.Debug("rejecting append entries request",
			"leader_id", req.LeaderID,
			"leader_term", req.LeaderTerm,
			"current_term", raft.currTerm,
			"our_log_contains_prefix", ourLogContainsPrefix)

		// [append ack] TODO: also communicate local log info to mitigate back and forth RPCs in case what sent us to this 'else' branch was the the lack of overlap with the prefix (instead of a term mismatch)
		res := AppendEntriesResponse{
			FollowerID:   raft.id,
			FollowerTerm: raft.currTerm,
			Ack:          0,
			Success:      false,
		}

		raft.network.SendAppendEntriesResponse(req.LeaderID, res)
	}
}

func (res AppendEntriesResponse) Handle(raft *Instance) {
	raft.slog.Debug("received append entries response",
		"current_term", raft.currTerm,
		"current_role", raft.role,
		"follower_id", res.FollowerID,
		"follower_term", res.FollowerTerm,
		"ack", res.Ack,
		"success", res.Success)

	if res.FollowerTerm > raft.currTerm {
		raft.slog.Debug("follower term is higher, transitioning to follower", "follower_id", res.FollowerID, "term", res.FollowerTerm, "current_term", raft.currTerm)
		raft.transitionToFollower(res.FollowerTerm)
		raft.restartElectionTimer()
	} else if res.FollowerTerm == raft.currTerm && raft.role == leader {
		if res.Success {
			// Invariant: Successful ack should not exceed log length
			assert.OK(res.Ack <= len(raft.log), "follower %d acked %d entries but log only has %d", res.FollowerID, res.Ack, len(raft.log))

			// If res.Ack <= raft.ackedLength[res.FollowerID] then even if res.Success is true
			// we don't need to do anything since we already have the ack for those entries, which means
			// this is a duplicate response.
			if res.Ack > raft.ackedLength[res.FollowerID] {
				oldSentLength := raft.sentLength[res.FollowerID]
				oldAckedLength := raft.ackedLength[res.FollowerID]

				raft.sentLength[res.FollowerID] = res.Ack
				raft.ackedLength[res.FollowerID] = res.Ack

				// Invariant: sentLength and ackedLength should advance together on success
				assert.OK(raft.sentLength[res.FollowerID] >= oldSentLength, "sentLength went backwards for follower %d: %d -> %d", res.FollowerID, oldSentLength, raft.sentLength[res.FollowerID])
				assert.OK(raft.ackedLength[res.FollowerID] >= oldAckedLength, "ackedLength went backwards for follower %d: %d -> %d", res.FollowerID, oldAckedLength, raft.ackedLength[res.FollowerID])

				raft.leaderCommitLogEntries()
			}
		} else if raft.sentLength[res.FollowerID] > 0 {
			// TODO: Improve this else branch which should use info from the follower's log to determine the new sentLength instead of
			// just decrementing it and potentially causing more back and forth RPCs
			raft.sentLength[res.FollowerID]--
			raft.replicateLog(res.FollowerID)
		}
	}
}

func (raft *Instance) transitionToFollower(newTerm uint64) {
	assert.OK(newTerm > raft.currTerm, "new term is not greater than current term: new_term = %d , current_term = %d", newTerm, raft.currTerm)

	raft.role = follower
	raft.currTerm = newTerm
	raft.votedFor = ZeroNodeID

	// Clean up any pending append requests
	for i, req := range raft.purgatory {
		select {
		case req.resultCh <- ErrNotLeader:
		default:
		}
		delete(raft.purgatory, i)
	}

	raft.persistState()
}

func (raft *Instance) persistState() {
	if err := raft.stateStore.Save(PersistentState{
		CurrentTerm: raft.currTerm,
		VotedFor:    raft.votedFor,
	}); err != nil {
		raft.slog.Error("failed to persist state", "error", err)
		panic(fmt.Errorf("raft instance %d failed to save state: %w", raft.id, err))
	}
}

func (raft *Instance) hasVotingMajority() bool {
	return raft.isVotingMajority(raft.votesReceived.Cardinality())
}

func (raft *Instance) isVotingMajority(value int) bool {
	return value > (len(raft.peers)+1)/2
}

func (raft *Instance) replicateLog(follower NodeID) {
	assert.OK(raft.role == leader, "attempting to replicate log as %s", raft.role)
	raft.slog.Debug("replicating log to follower", "follower", follower, "term", raft.currTerm)

	prefixLength := raft.sentLength[follower]
	suffix := raft.log[prefixLength:]

	var prefixTerm uint64
	if prefixLength > 0 {
		prefixTerm = raft.log[prefixLength-1].Term
	}

	raft.slog.Debug("sending append entries request",
		"follower", follower,
		"term", raft.currTerm,
		"prefix_length", prefixLength,
		"prefix_term", prefixTerm,
		"suffix_length", len(suffix))
	raft.network.SendAppendEntriesRequest(follower, AppendEntriesRequest{
		LeaderID:           raft.id,
		LeaderTerm:         raft.currTerm,
		LeaderCommitLength: raft.commitLength,
		PrefixLength:       prefixLength,
		PrefixTerm:         prefixTerm,
		Suffix:             suffix,
	})
}

func (raft *Instance) periodicReplication() {
	if raft.role == leader {
		raft.slog.Debug("periodic leader replication of log to followers", "term", raft.currTerm)
		for _, follower := range raft.peers {
			raft.replicateLog(follower)
		}
	} else {
		raft.slog.Debug("not leader, stopping periodic log replication")
		raft.leaderHeartbeatTicker.Stop()
	}
}

// Pre-Condition: ourLogContainsPrefix
// This means that raft.log == [...prefix, ...maybe_some_other_entries]
func (raft *Instance) appendEntries(prefixLength, leaderCommitLength int, suffix []LogEntry) {
	// The leader's complete log is the prefix + suffix
	leaderLogLength := prefixLength + len(suffix)

	// If len(raft.log) > prefixLength then we know that the log is identical to the prefix up to raft.log[prefixLength-1].
	// If len(suffix) > 0 we know the leader believes that there are some new entries we have not appended to our log yet.
	// Putting the two together, we know that there's an overlap between our current log and the 'suffix' part of the leader's log
	// (if we consider leader_log = prefix + suffix).
	// The entries in this overlap might be identical to the entries in the part of the suffix they are overlapping with, or not.
	// And if they are not, we need to remove the conflicting entries from our log before appending the new ones.
	// In other words, we MIGHT have to trim our current log before potentially appending new entries.
	if len(raft.log) > prefixLength && len(suffix) > 0 {
		// We first need to consider the last log position we are sure overlaps both logs (ours and the leader's).
		// To do this, we assume the position of the last entry in the smallest log (because there's still a chance our log is
		// bigger than the leader's, and in that case, whichever goes past the overlap should immediately be trimmed).
		idx := min(len(raft.log), leaderLogLength) - 1

		// Now we know idx is the last log index to consider. Everything after it is garbage.
		// Now we can perform a very cheap check to understand whether our last entry is already from the current leader or not.
		// If not, then we can trim the log up to prefixLength because whatever goes past it (if anything) can be conflicting
		// with the leader's log.
		// In the case these entries are from previous terms, it just means they were never committed. Otherwise they would be
		// part of the leader's log as well.
		if raft.log[idx].Term != suffix[idx-prefixLength].Term {
			// Trim the log from prefixLength onwards
			raft.log = raft.log[:prefixLength]
		}
	}

	// At this point we know it's safe to append new entries to our log because we've trimmed any conflicting entries.
	// If we kept anything after the prefixLength then it means it was identical to the leader's log (e.g. the entries
	// in the suffix).
	//
	// We now check if there are any new entries to append.
	// If the leader's log is bigger than our log, then we know there are new entries to append.
	if leaderLogLength > len(raft.log) {
		// If we got here it means that we either contain no entries from suffix, or some of them.
		// Whichever the case, we need to append the ones missing.
		// The index startIndex := len(raft.log) - prefixLength is the first entry in the suffix that we need to append
		// E.g. if our log is [1, 2, 3], prefixLength is 3 and the leader's log is [1, 2, 3, 4, 5]
		// then we need to append [4, 5] to our log and startIndex := 3 - 3 = 0
		startIndex := len(raft.log) - prefixLength
		for i := startIndex; i < len(suffix); i++ {
			raft.log = append(raft.log, suffix[i])
		}

		raft.persistNewEntries(raft.log[startIndex:])
	}

	// At this point we have either appended some new entries to our log or none. But either way,
	// our log should be consistent with the leader's
	// (we might still have kept some outdated entries at the end of our log if suffix was empty).
	//
	// We should now check if we can advance the commit pointer and deliver new entries to the
	// state machine.
	if leaderCommitLength > raft.commitLength {
		// We deliver the committed entries BEFORE we update and persist the commitLength.
		// This means that we might deliver the same entries more than once (e.g. if the application
		// crashes before we successfully persist the commitLength).
		// This is by design, because if we did things the other way around and still crashed, we would
		// not deliver the cmd even once. That way, we prefer at-least-once delivery over at-most-once.
		// It is up to the state machine to be idempotent and handle duplicates in order to achieve
		// exactly-once delivery.
		for i := raft.commitLength; i < leaderCommitLength; i++ {
			raft.deliver(CommittedEntry{
				LogSeqNum: i,
				Cmd:       raft.log[i].Cmd,
			})
		}

		raft.commitLength = leaderCommitLength
	}
}

func (raft *Instance) leaderCommitLogEntries() {
	// Invariant: Should only commit entries as leader
	assert.OK(raft.role == leader, "attempting to commit entries as %s", raft.role)

	// We iterate over the log and check if we can deliver any new entries to the state machine
	for raft.commitLength < len(raft.log) {
		currLogEntryTerm := raft.log[raft.commitLength].Term

		acks := 1 // our ack
		for _, follower := range raft.peers {
			if raft.ackedLength[follower] > raft.commitLength {
				acks++
			}
		}

		// We need to check if we have a majority of acks for the current entry
		// If we do, we can deliver it to the state machine and advance the commitLength
		// Otherwise, there's nothing else to commit
		if raft.isVotingMajority(acks) {
			// Invariant: Should only commit entries from current term (Leader Completeness)
			assert.OK(currLogEntryTerm == raft.currTerm, "leader attempting to commit entry from old term %d != %d", currLogEntryTerm, raft.currTerm)

			raft.deliver(CommittedEntry{
				LogSeqNum: raft.commitLength,
				Cmd:       raft.log[raft.commitLength].Cmd,
			})

			if req, ok := raft.purgatory[raft.commitLength]; ok {
				select {
				case req.resultCh <- nil:
				default:
				}
				delete(raft.purgatory, raft.commitLength)
			}

			raft.commitLength++
		} else {
			break
		}
	}
}

func (raft *Instance) persistNewEntries(entries []LogEntry) {
	if err := raft.logStore.Append(entries); err != nil {
		raft.slog.Error("failed to persist new entries", "error", err)
		panic(fmt.Errorf("raft instance %d failed to persist new entries: %w", raft.id, err))
	}
}

func (raft *Instance) deliver(entry CommittedEntry) {
	if err := raft.fsm.Apply(entry); err != nil {
		raft.slog.Error("failed to deliver entry", "error", err)
		panic(fmt.Errorf("raft instance %d failed to deliver entry: %w", raft.id, err))
	}
}

// TODO: Improvements
// There are a great deal of improvements to be done here.
// When we have a proper Log implementation that writes to disk then we can revisit this since right now our log is just a simple slice in memory.
// In the future we should have a high-performance, disk-based log which is akin to Kafka's implementation.
// This will allow us to write to the filesystem directly here with very low latency (potentially using buffered IO)
// and have a confurable flush/fsync.
// We could then configure this component under the hood to signal the raft.Network instance, so it knows when to flush the network requests as well.
//
// This new Log abstraction should combine:
// 1. raft.log = append(...)
// 2. raft.persistNewEntries(raft.log[len(raft.log)-1:])
//
// The calls to raft.Network are assumed to be async anyway (just scheduling, not actually sending).
// So those can be delayed arbitrarily, e.g. they can be sync'd with the disk flush.
// The only correctness requirement here is that we only send AppendEntries RPCs with entries persisted to disk.
func (raft *Instance) appendLogEntry(req appendRequest) {
	if raft.role != leader {
		raft.slog.Warn("attempted to append log entry as non-leader", "role", raft.role, "term", raft.currTerm)
		req.resultCh <- ErrNotLeader // safe because the channel is buffered
		return
	}

	raft.slog.Debug("appending new log entry as leader", "term", raft.currTerm)

	if len(raft.purgatory) >= raft.cfg.MaxPendingAppendRequests {
		raft.slog.Warn("max pending append requests reached, dropping request", "max_pending_append_requests", raft.cfg.MaxPendingAppendRequests)
		req.resultCh <- ErrMaxPendingAppendRequestsReached // safe because the channel is buffered
		return
	}

	raft.purgatory[len(raft.log)] = req

	raft.log = append(raft.log, LogEntry{
		Term: raft.currTerm,
		Cmd:  req.cmd,
	})

	raft.persistNewEntries(raft.log[len(raft.log)-1:])
	for _, follower := range raft.peers {
		raft.replicateLog(follower)
	}

	// Restart heartbeat ticker to avoid unnecessary network traffic by sending
	// heartbeats too close to AppendEntriesRequests that were just sent with new entries.
	raft.leaderHeartbeatTicker.Reset(raft.cfg.LeaderHeartbeatInterval)
}
