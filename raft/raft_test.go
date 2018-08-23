package raft

import (
	pb "github.com/zhaohaidao/raft-go/raft/raftpb"
	"fmt"
)

func newTestRaft(id uint64, peers []uint64, election, heartbeat int, storage Storage) *raft {
	return newRaft(newTestConfig(id, peers, election, heartbeat, storage))
}

func newTestConfig(id uint64, peers []uint64, election, heartbeat int, storage Storage) *Config {
	return &Config{
		ID:              id,
		peers:           peers,
		ElectionTick:    election,
		HeartbeatTick:   heartbeat,
		Storage:         storage,
	}
}

func idsBySize(size int) []uint64 {
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		ids[i] = 1 + uint64(i)
	}
	return ids
}

func (r *raft) readMessages() []pb.Message {
	msgs := r.msgs
	r.msgs = make([]pb.Message, 0)

	return msgs
}

type stateMachine interface {
	Step(m pb.Message) error
	readMessages() []pb.Message
}

type network struct {
	peers   map[uint64]stateMachine
	storage map[uint64]*memoryStorage
	dropm   map[connem]float64
	ignorem map[pb.MessageType]bool

	// msgHook is called for each message sent. It may inspect the
	// message and return true to send it or false to drop it.
	msgHook func(pb.Message) bool
}

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *stateMachine.
// A *stateMachine will get its k, id.
// When using stateMachine, the address list is always [1, n].
func newNetwork(peers ...stateMachine) *network {
	return newNetworkWithConfig(nil, peers...)
}

// newNetworkWithConfig is like newNetwork but calls the given func to
// modify the configuration of any state machines it creates.
func newNetworkWithConfig(configFunc func(*Config), peers ...stateMachine) *network {
	size := len(peers)
	peerAddrs := idsBySize(size)

	npeers := make(map[uint64]stateMachine, size)
	nstorage := make(map[uint64]*memoryStorage, size)

	for j, p := range peers {
		id := peerAddrs[j]
		switch v := p.(type) {
		case nil:
			nstorage[id] = NewMemoryStorage()
			cfg := newTestConfig(id, peerAddrs, 10, 1, nstorage[id])
			if configFunc != nil {
				configFunc(cfg)
			}
			sm := newRaft(cfg)
			npeers[id] = sm
		case *raft:
			v.id = id
			v.prs = make(map[uint64]*Progress)
			for i := 0; i < size; i++ {
				v.prs[peerAddrs[i]] = &Progress{}
			}
			v.reset(v.Term)
			npeers[id] = v
		case *blackHole:
			npeers[id] = v
		default:
			panic(fmt.Sprintf("unexpected state machine type: %T", p))
		}
	}
	return &network{
		peers:   npeers,
		storage: nstorage,
		dropm:   make(map[connem]float64),
		ignorem: make(map[pb.MessageType]bool),
	}
}

func (nw *network) send(msgs ...pb.Message) {
}

type connem struct {
	from, to uint64
}

type blackHole struct{}

var nopStepper = &blackHole{}

func (blackHole) Step(pb.Message) error      { return nil }
func (blackHole) readMessages() []pb.Message { return nil }
