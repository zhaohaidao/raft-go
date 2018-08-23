// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/zhaohaidao/raft-go/raft/raftpb"
)


// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
type Storage interface {
	// InitialState returns the saved HardState and ConfState information.
	InitialState() (pb.HardState, pb.ConfState, error)
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)
}

type memoryStorage struct {

}

func (m *memoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return pb.HardState{}, pb.ConfState{}, nil
}

func (m *memoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	return []pb.Entry{}, nil
}

func (m *memoryStorage) Term(i uint64) (uint64, error) {
	return 0, nil
}

func (m *memoryStorage) LastIndex() (uint64, error)  {
	return 0, nil
}

func (m *memoryStorage) FirstIndex() (uint64, error)  {
	return 0, nil
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() *memoryStorage {
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
func (ms *memoryStorage) Append(entries []pb.Entry) error {
	return nil
}

