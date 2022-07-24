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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {

	var hardState, _, err = storage.InitialState()
	if err != nil {
		panic(err)
	}

	var lo, _ = storage.FirstIndex()
	var hi, _ = storage.LastIndex() // 这个返回的是最后一个有效的Entity的索引

	var entities []pb.Entry
	if lo <= hi {
		entities, err = storage.Entries(lo, hi+1)
		if err != nil {
			panic(err)
		}
	}

	return &RaftLog{
		storage:    storage,
		committed:  hardState.Commit,
		applied:    lo - 1,
		stabled:    hi,
		entries:    entities,
		firstIndex: lo,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	var first, _ = l.storage.FirstIndex()
	var start = int(l.stabled - first + 1)
	if start < 0 {
		return nil
	}
	return l.entries[start:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.applied > l.committed {
		return nil
	}
	var first, _ = l.storage.FirstIndex()
	var begin, end = int(l.applied + 1 - first), int(l.committed + 1 - first)
	if begin < 0 || end > len(l.entries) {
		return nil
	}
	return l.entries[begin:end]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	var idx uint64
	if len(l.entries) == 0 {
		// buf, why?
		// RaftLog.entries来自于Storage中的[lo, hi]数据,
		// 如果为空, 那么就等同于有效索引从 FirstIndex 开始(?)
		// storage 的 ents长度不可能为0, 只有在
		idx, _ = l.storage.FirstIndex()
		// -1 是因为底层返回的是一个+1的数(???)
		idx -= 1
	} else {
		idx = l.entries[len(l.entries)-1].Index
	}
	return idx
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// 先从有效的集合中获取
	if len(l.entries) > 0 && i >= l.firstIndex {
		if i > l.LastIndex() {
			return 0, ErrUnavailable
		}
		return l.entries[i-l.firstIndex].GetTerm(), nil
	}
	return l.storage.Term(i)
}

func (l *RaftLog) appendEntries(entries []pb.Entry) uint64 {
	if len(entries) == 0 {
		return l.LastIndex()
	}
	l.entries = append(l.entries, entries...)
	return l.LastIndex()
}

func (l *RaftLog) commitTo(to uint64) bool {
	if l.committed >= to {
		return false
	}
	if l.LastIndex() < to {
		return false
	}
	l.committed = to
	return true
}

func (l *RaftLog) entriesSlice(i uint64) []pb.Entry {
	if i > l.LastIndex() {
		return nil
	}
	var first, _ = l.storage.FirstIndex()
	if i < first {
		return nil
	}
	return l.entries[i-first:]
}

func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
	if lo >= l.firstIndex && hi-lo <= uint64(len(l.entries)) {
		return l.entries[lo-l.firstIndex : hi-l.firstIndex]
	}
	ent, _ := l.storage.Entries(lo, hi)
	return ent
}

func (l *RaftLog) RemoveEntriesAfter(lo uint64) {
	// 移除从lo 开始的所有日志
	// 先看已存储的部分
	l.stabled = min(l.stabled, lo-1)

	// 处理未 **压缩的条目**
	if lo-l.firstIndex >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:lo-l.firstIndex]
}
