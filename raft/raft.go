package raft

import (
	"errors"
	"fmt"
	pb "github.com/zhaohaidao/raft-go/raft/raftpb"
	"math/rand"
	"strings"
	"sync"
	"time"
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
	}
	//else if m.Term < r.Term {
	//	// Actually it is ok to ignore this message if msg type is Vote
	//	// raft vote requirement(section5.2): If votes received from majority of servers: become leader
	//	// ignore action has the same effect as reject action
	//	return nil
	//}

	switch m.Type {
	case pb.MsgHup:
		// candidate received
		if r.state != StateLeader {
			r.campaign()
			r.maybeGranted()
		}
	case pb.MsgVote:
		// all server received
		if r.state != StateLeader {
			reject := true
			if r.Vote == None {
				reject = !r.raftLog.isUpdateTo(m.LogTerm, m.Index)
			} else {
				reject = r.Vote != m.From
			}
			r.send(pb.Message{Type:pb.MsgVoteResp, From:r.id, To:m.From, Reject:reject})
		}
	case pb.MsgVoteResp:
		// candidates received
		if r.state == StateCandidate {
			r.votes[m.From] = !m.Reject
			r.maybeGranted()
		}
	case pb.MsgBeat:
		if r.state == StateLeader {
			r.bcastHeartbeat()
		}
	case pb.MsgProp:
		if r.state == StateLeader {
			r.appendEntry(m.Entries...)
			r.bcastAppend()
			r.maybeCommitted(r.raftLog.lastIndex(), m.Term)
		}
	case pb.MsgApp:
		if r.state == StateCandidate {
			r.becomeFollower(m.Term, m.From)
		} else if r.state == StateFollower {
			term, err := r.raftLog.term(m.Index)
			reject := err == ErrUnavailable || m.LogTerm != term
			resp := pb.Message{Type: pb.MsgAppResp, From: r.id, To: m.From, Reject: reject}
			if reject {
				resp.RejectHint = r.raftLog.lastIndex()
				resp.Index = m.Index
				r.send(resp)
				return nil
			}
			r.raftLog.append(m.Entries...)
			// TODO: 弄清楚pb.Message里Index的意思
			// MsgApp: prevLogIndex
			// MsgAppResp: reject=false时，表示NextMatch；reject=true时呢？原样返回，无语
			// MsgApp的resp里的Index应该表示最新的Match，为什么Index小于Committed时，要返回Committed？
			// etcd-raft的handleAppendEntries里，有如下逻辑（raft论文未作解释）。
			// 	if m.Index < r.raftLog.committed {
			//		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
			//		return
			//	}
			// 为了通过TestFollowerCheckMsgApp的测试，这里沿用etcd的逻辑
			// 另外，为了保证case通过， 参考etcd，rejectHint会返回lastIndex
			if m.Index < r.raftLog.committed {
				resp.Index = r.raftLog.committed
				r.send(resp)
				return nil
			}
			resp.Index = m.Index + uint64(len(m.Entries))
			if m.Commit > r.raftLog.committed {
				r.raftLog.committed = min(m.Commit, r.raftLog.lastIndex())
			}
			// leader keeps append entries until the follower's log is consistent with leader's
			// this logic should be in send end
			if r.raftLog.committed != m.Commit {
				r.send(resp)
			}
		}
	case pb.MsgAppResp:
		if r.state == StateLeader {
			if !m.Reject {
				term, err := r.raftLog.term(m.Index)
				if err != nil {
					r.logger.Panicf("term not found for index: %v", m.Index)
				}
				// once an entry from the current term has been committed in this way, then all prior entries are
				// committed indirectly
				if term == r.Term &&
					m.Index > r.prs[m.From].Match &&
					r.raftLog.lastIndex() >= r.prs[m.From].Next {

					r.prs[m.From].Match = m.Index
					r.prs[m.From].Next = m.Index + 1
					r.maybeCommitted(m.Index, m.Term)
				}
			} else {
				r.prs[m.From].Next -= 1
			}

			next := r.prs[m.From].Next
			prev := next - 1
			prevTerm, err := r.raftLog.term(prev)
			if err != nil {
				r.logger.Panicf("term not found for prev index: %v", prev)
			}
			r.send(pb.Message{
				Type:    pb.MsgApp,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Index:   prev,
				LogTerm: prevTerm,
				Commit:  r.raftLog.committed,
				Entries: r.entriesByNext(next),
			})
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
	r.Term = state.Term
	r.Vote = state.Vote
	r.raftLog.committed = state.Commit
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
	r.state = StateFollower
	r.reset(term)
	r.lead = lead
	r.Vote = None
	r.tick = r.tickElection
}

func (r *raft) becomeCandidate() {
	r.state = StateCandidate
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.votes[r.id] = true
	r.tick = r.tickElection
}

func (r *raft) becomeLeader() {
	r.state = StateLeader
	r.reset(r.Term)
	r.Vote = None
	r.tick = r.tickHeartbeat
	// reset Next first or append no-op entry?
	// why not initialize NextIndex to lastIndex directly?
	for _, pr := range r.prs {
		pr.Match = 0
		pr.Next = r.raftLog.lastIndex() + 1
	}
	r.appendEntry(pb.Entry{
		Term:  r.Term,
		Index: r.raftLog.lastIndex() + 1,
		Data:  nil,
	})
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
	}
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
	for id := range r.votes {
		delete(r.votes, id)
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
	r.raftLog.append(es...)

}

func (r *raft) campaign() {
	// prepare args
	// foreach MsgVote
	r.becomeCandidate()
	lastTerm := r.raftLog.lastTerm()
	lastIndex := r.raftLog.lastIndex()
	for i := range r.prs {
		// raft required candidate votes himself as the leader
		if r.id != i {
			r.send(pb.Message{
				Type:    pb.MsgVote,
				To:      i,
				From:    r.id,
				Term:    r.Term,
				LogTerm: lastTerm,
				Index:   lastIndex,
			})
		}
	}
}

func (r *raft) send(m pb.Message) {
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
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
	if r.state == StateLeader {
		panic("tickElection should never happen when it is the leader")
	}
	r.electionElapsed += 1

	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	}
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
	r.votes = map[uint64]bool{}

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
	var nodes []uint64
	for p := range r.prs {
		nodes = append(nodes, p)
	}
	return nodes
}

func (r *raft) entriesByNext(next uint64) []pb.Entry {
	entries := r.raftLog.allEntries()
	for index := range entries {
		if entries[index].Index == next {
			return entries[index:]
		}
	}
	return nil

}

func (r *raft) bcastHeartbeat() {
	if r.state != StateLeader {
		panic("bcastHeartbeat should never happen when it is not leader")
	}

	for i := range r.prs {
		if r.id != i {
			r.send(pb.Message{
				Type:    pb.MsgHeartbeat,
				From:    r.id,
				To:      i,
				Term:    r.Term,
			})
		}
	}
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (r *raft) bcastAppend() {
	if r.state != StateLeader {
		panic("bcastAppend should never happen when it is not leader")
	}

	// TODO: implement allEntries
	commited := r.raftLog.committed
	for i, p := range r.prs {
		if r.id != i {
			prev := p.Next - 1
			prevTerm, err := r.raftLog.term(prev)
			if err != nil {
				r.logger.Panicf("term not found for prev index: %v", prev)
			}
			r.send(pb.Message{
				Type:    pb.MsgApp,
				From:    r.id,
				To:      i,
				Term:    r.Term,
				Index:   prev,
				LogTerm: prevTerm,
				Commit:  commited,
				Entries: r.entriesByNext(p.Next),
			})
		}
	}
}

func (r *raft) maybeGranted() {
	grantCount := 0
	for peer := range r.votes {
		if r.votes[peer] {
			grantCount += 1
		}
	}
	if grantCount >= len(r.prs)/2 + 1 {
		r.becomeLeader()
		return
	}
}

func (r *raft) maybeCommitted(index uint64, term uint64) {
	successCount := 1
	for i, p := range r.prs {
		if r.id != i {
			if p.Match >= index && r.Term == term {
				successCount += 1
			}
		}
		if successCount >= len(r.prs)/2 + 1 {
			r.raftLog.committed = index
			return
		}

	}
}

