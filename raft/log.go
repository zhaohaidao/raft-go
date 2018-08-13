package raft

import (
	pb "github.com/coreos/etcd/raft/raftpb"
)

type raftLog struct {
	// all entries that have not yet been written to storage.
	entries []pb.Entry

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64
	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	logger Logger
}
