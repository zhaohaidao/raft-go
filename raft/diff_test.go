package raft

import "fmt"

func ltoa(l *raftLog) string {
	s := fmt.Sprintf("committed: %d\n", l.committed)
	s += fmt.Sprintf("applied:  %d\n", l.applied)
	for i, e := range l.allEntries() {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}

func diffu(a, b string) string {
	return ""
}

