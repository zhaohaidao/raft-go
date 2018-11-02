package raft

import (
	pb "github.com/zhaohaidao/raft-go/raft/raftpb"
	"log"
	"math"
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
		storage: storage,
		logger:  logger,
	}
	return log
}

func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	prev := ents[0].Index - 1
	if prev < l.committed {
		l.logger.Panicf("prev(%d) is out of range [committed(%d)]", prev, l.committed)
	}
	// setion 5.3
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all
	// that follow it
	if l.lastIndex() > prev {
		for i, entry := range ents {
			term, err := l.term(entry.Index)
			if err != nil {
				panic(err)
			}
			if entry.Term != term {
				if len(l.entries) > 0 {
					l.entries = l.slice(l.entries[0].Index, entry.Index)
				}
				l.entries = append(l.entries, ents[i:]...)
				break
			}
		}
	} else {
		l.entries = append(l.entries, ents...)
	}
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
	if i > l.committed {
		l.logger.Panicf("applied should never be less than committed", i, l.committed)
	}
	l.applied = i
}

func (l *raftLog) lastIndex() uint64 {
	len := len(l.entries)
	if len > 0 {
		return l.entries[len-1].Index
	}
	index, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

func (l *raftLog) lastTerm() uint64 {
	len := len(l.entries)
	if len > 0 {
		return l.entries[len-1].Term
	}
	term, err := l.storage.Term(l.lastIndex())
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return term
}

func (l *raftLog) term(i uint64) (uint64, error) {
	if len(l.entries) == 0 || i < l.entries[0].Index {
		return l.storage.Term(i)
	} else {
		for index := range l.entries {
			if l.entries[index].Index == i {
				return l.entries[index].Term, nil
			}
		}
		return 0, ErrUnavailable
	}
}

// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
// note: available entries means committed entries
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	lo := l.applied + 1
	hi := l.committed + 1
	if lo >= l.entries[0].Index {
		return l.slice(lo, hi)
	} else {
		last, err := l.storage.LastIndex()
		if err != nil {
			l.logger.Panicf("failed to get storage last index")
		}
		var ents []pb.Entry
		if hi < last - 1 {
			ents, err = l.storage.Entries(lo, hi, math.MaxUint64)
			if err != nil {
				l.logger.Panicf("failed to get storage entires")
			}
		} else {
			ents, err = l.storage.Entries(lo, last + 1, math.MaxUint64)
			if err != nil {
				l.logger.Panicf("failed to get storage entires")
			}
			ents = append(ents, l.slice(last+1, hi)...)
		}
		return ents
	}
}

func (l *raftLog) slice(lo uint64, hi uint64) []pb.Entry {
	size := uint64(hi - lo)
	loIndex := -1
	for i, ent := range l.entries {
		if ent.Index == lo {
			loIndex = i
		}
	}
	if loIndex == -1 {
		log.Panicf("low index not found in unstable entries")
	}
	return l.entries[loIndex : uint64(loIndex)+size]

}

// allEntries returns all entries in the log.
func (l *raftLog) allEntries() []pb.Entry {
	lo, err := l.storage.FirstIndex()
	if err == ErrUnavailable {
		return l.entries
	} else if err != nil {
		l.logger.Panicf("Get first index failed. err: %v", err)
	}
	var hi uint64
	if len(l.entries) > 0 {
		hi = l.entries[0].Index
	} else {
		lastIndex, err := l.storage.LastIndex()
		if err != nil {
			l.logger.Panic("Get last index failed. err: %v", err)
		}
		hi = lastIndex + 1
	}
	all, err := l.storage.Entries(lo, hi, math.MaxUint64)
	if err != nil {
		l.logger.Panicf("Get entries failed. lo: %d, hi: %d, err: %v", lo, hi, err)
	}
	all = append(all, l.entries...)
	return all
}

func (l *raftLog) unstableEntries() []pb.Entry {
	return l.entries
}

func (l *raftLog) stableTo(i, t uint64) {
	var newUnstable []pb.Entry
	for index, entry := range l.entries {
		if entry.Index == i && entry.Term == t {
			newUnstable = l.entries[0:index]
			newUnstable = append(newUnstable, l.entries[index+1:]...)
			l.entries = newUnstable
			return
		}
	}
}
