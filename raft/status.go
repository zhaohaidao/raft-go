package raft

import (
	"fmt"

	pb "github.com/coreos/etcd/raft/raftpb"
)

type Status struct {
	ID uint64

	pb.HardState
	SoftState

	Applied  uint64

	LeadTransferee uint64
}

// getStatus gets a copy of the current raft status.
func getStatus(r *raft) Status {
	s := Status{
		ID:             r.id,
		LeadTransferee: r.leadTransferee,
	}

	s.HardState = r.hardState()
	s.SoftState = *r.softState()

	s.Applied = r.raftLog.applied

	return s
}

// MarshalJSON translates the raft status into JSON.
// TODO: try to simplify this by introducing ID type into raft
func (s Status) MarshalJSON() ([]byte, error) {
	j := fmt.Sprintf(`{"id":"%x","term":%d,"vote":"%x","commit":%d,"lead":"%x","raftState":%q,"applied":%d}`,
		s.ID, s.Term, s.Vote, s.Commit, s.Lead, s.RaftState, s.Applied)
	return []byte(j), nil
}

func (s Status) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		raftLogger.Panicf("unexpected error: %v", err)
	}
	return string(b)
}
