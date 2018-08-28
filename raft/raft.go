package raft

import (
	pb "github.com/zhaohaidao/raft-go/raft/raftpb"
	"fmt"
	"strings"
	"errors"
	"math/rand"
	"time"
	"sync"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	numStates
)

// StateType represents the role of a node in a cluster.
type StateType uint64

type stepFunc func(r *raft, m pb.Message) error

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

type raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	raftLog *raftLog

	maxInflight int
	maxMsgSize  uint64
	prs         map[uint64]*Progress

	state StateType

	// isLearner is true if the local raft node is a learner.
	isLearner bool

	votes map[uint64]bool

	msgs []pb.Message

	// the leader id
	lead uint64

	// number of ticks since it reached last electionTimeout when it is candidate
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

	tick func()
	step stepFunc

	logger Logger
}

// TODO: leader election
func (r *raft) Step(m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	} else if m.Term < r.Term {
		// Actually it is ok to ignore this message if msg type is Vote
		// raft vote requirement(section5.2): If votes received from majority of servers: become leader
		// ignore action has the same effect as reject action
		return nil
	}

	switch m.Type {
	case pb.MsgHup:
		// candidate received
		if r.state == StateCandidate {
			r.campaign()
		}
	case pb.MsgVote:
		// all server received
		if r.state != StateLeader {
			reject := !r.raftLog.isUpdateTo(m.LogTerm, m.Index)
			r.send(pb.Message{Type:pb.MsgVoteResp, From:r.id, To:m.From, Reject:reject})
		}
	case pb.MsgVoteResp:
		// candidates received
		if r.state == StateCandidate {
			r.votes[m.From] = !m.Reject
			r.maybeGranted()
		}
	default:

	}
	return nil
}

func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}
func (r *raft) loadState(state pb.HardState) {
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}

	return nil
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.lead = lead
	r.state = StateFollower
	r.Vote = None

}

func (r *raft) becomeCandidate() {
	r.state = StateCandidate
	r.Vote = r.id
	r.reset(r.Term + 1)
	r.Step(pb.Message{Type:pb.MsgHup})
}

func (r *raft) becomeLeader() {
	r.reset(r.Term)
	r.state = StateLeader
	r.Vote = None
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
	}
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
	for _, pr := range r.prs {
		pr.Match = 0
		pr.Next = r.raftLog.lastIndex() + 1
	}
}

func stepFollower(r *raft, m pb.Message) error {
	return nil
}

func stepCandidate(r *raft, m pb.Message) error {
	return nil
}

func stepLeader(r *raft, m pb.Message) error {
	return nil
}

func (r *raft) appendEntry(es ...pb.Entry) {
	lastIndex := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = lastIndex + 1 + uint64(i)
	}
	r.raftLog.append()

}

func (r *raft) campaign() {
	// prepare args
	// foreach MsgVote
	for i := range r.prs {
		// raft required candidate votes himself as the leader
		r.send(pb.Message{From: r.id, To: i, Type: pb.MsgVote})
	}
}

func (r *raft) send(m pb.Message) {
	m.Term = r.Term

}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	if r.state != StateLeader {
		return
	}
	r.Step(pb.Message{From: r.id, Type: pb.MsgBeat})
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {

}


func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLog(c.Storage, c.Logger)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	peers := c.peers
	if len(cs.Nodes) > 0 {
		if len(peers) > 0 {
			// TODO(bdarnell): the peers argument is always nil except in
			// tests; the argument should be removed and these tests should be
			// updated to specify their nodes through a snapshot.
			panic("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)")
		}
		peers = cs.Nodes
	}
	r := &raft{
		id:                        c.ID,
		lead:                      None,
		isLearner:                 false,
		raftLog:                   raftlog,
		prs:                       make(map[uint64]*Progress),
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		logger:                    c.Logger,
	}
	for _, p := range peers {
		r.prs[p] = &Progress{Next: 1}
	}

	if !isHardStateEqual(hs, emptyState) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) nodes() []uint64 {
	return []uint64{}
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (r *raft) bcastAppend() {
}

func (r *raft) maybeGranted() {
	grantCount := 0
	for peer := range r.votes {
		if r.votes[peer] {
			grantCount += 1
		}
	}
	if grantCount >= len(r.votes)/2 + 1 {
		r.becomeLeader()
	}
}

