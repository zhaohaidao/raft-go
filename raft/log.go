package raft

import (
	pb "github.com/zhaohaidao/raft-go/raft/raftpb"
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

func (l *raftLog) appliedTo(i uint64) {
}

func (l *raftLog) lastIndex() uint64 {
	return 0
}

func (l *raftLog) lastTerm() uint64 {
	return 0
}

// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	return []pb.Entry{}
}

// allEntries returns all entries in the log.
func (l *raftLog) allEntries() []pb.Entry {
	return []pb.Entry{}
}

func (l *raftLog) unstableEntries() []pb.Entry {
	return []pb.Entry{}
}

func (l *raftLog) stableTo(i, t uint64) {

}
