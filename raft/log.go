package raft

import (
	pb "github.com/zhaohaidao/raft-go/raft/raftpb"
	"log"
)

type raftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

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

// newLog returns a log using the given storage
func newLog(storage Storage, logger Logger) *raftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &raftLog{
		storage:    storage,
		logger:     logger,
	}
	return log
}

func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	after := ents[0].Index - 1
	if after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	if l.lastIndex() > after {
		l.entries = l.entries[0 : after + 1]
	}
	l.entries = append(l.entries, ents...)
	return l.lastIndex()
}

func (l *raftLog) isUpdateTo(lastLogTerm uint64, lastLogIndex uint64) bool {
	if lastLogTerm < l.lastTerm() {
		return false
	} else if lastLogTerm > l.lastTerm() {
		return true
	} else {
		return lastLogIndex >= l.lastIndex()
	}
}

func (l *raftLog) appliedTo(i uint64) {
}

func (l *raftLog) lastIndex() uint64 {
	index, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

func (l *raftLog) lastTerm() uint64 {
	term, err := l.storage.Term(l.lastIndex())
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return term
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
