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
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

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

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	// Match: 当前已同步的位置
	// Next: 下一个要同步的位置
	Match, Next uint64
}

func (p *Progress) MaybeUpdate(n uint64) bool {
	var update bool
	if p.Match < n {
		p.Match = n
		update = true
	}
	p.Next = max(p.Next, n+1)
	return update
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	randomElectionElapsed int // 随机值

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	var hardState, confState, _ = c.Storage.InitialState()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	var r = &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	r.RaftLog.applied = c.Applied
	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{}
	}
	// r.electionElapsed = rand.Intn(r.electionTimeout)
	r.becomeFollower(r.Term, None)
	return r
}

func (r *Raft) send(msg pb.Message) {
	//TODO 添加其他规则
	msg.From = r.id
	msg.Term = r.Term
	r.msgs = append(r.msgs, msg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	var pr = r.Prs[to]
	if pr == nil {
		return false
	}
	var term, _ = r.RaftLog.Term(pr.Next - 1)
	var entries = r.RaftLog.entriesSlice(pr.Next)

	if len(entries) == 0 {
		return false
	}

	var msg pb.Message
	msg.To = to
	msg.MsgType = pb.MessageType_MsgAppend
	msg.Index = pr.Next - 1
	msg.LogTerm = term
	msg.Commit = r.RaftLog.committed
	msg.Entries = make([]*pb.Entry, len(entries))
	for i, e := range entries {
		msg.Entries[i] = &e
	}
	r.send(msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
	})
}

func (r *Raft) sendHeartbeatResp(to uint64) {
	r.send(pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		// Leader节点发送心跳给Follower节点
		r.heartbeatElapsed++
		if r.heartbeatElapsed < r.heartbeatTimeout {
			return
		}
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if !r.pastElectionTimeout() {
			return
		}
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		return
	}
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.votes[r.id] = true
	r.Vote = r.id
	// 只有一个节点时, 直接成功Leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id

	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
	}

	r.Step(pb.Message{
		From:    r.id,
		To:      r.id,
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = map[uint64]bool{}
	r.resetRandomElection()
}

func (r *Raft) resetRandomElection() {
	r.randomElectionElapsed = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomElectionElapsed
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.GetMsgType() {
		case pb.MessageType_MsgHup:
			r.requestElection()
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.GetMsgType() {
		case pb.MessageType_MsgHup:
			r.requestElection()
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateLeader:
		switch m.GetMsgType() {
		case pb.MessageType_MsgBeat:
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
			r.heartbeatElapsed = 0
		case pb.MessageType_MsgAppend:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
				r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		}
	}
	return nil
}

func (r *Raft) requestElection() {
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
		})
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.MsgType != pb.MessageType_MsgAppendResponse {
		return
	}

	var pr = r.Prs[m.From]
	if pr == nil {
		return
	}

	if m.Reject {

		return
	}

	if pr.MaybeUpdate(m.Index) {
		if r.maybeCommit() {
			r.broadcastAppend()
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.Prs[m.From] == nil {
		return
	}

	var entries = make([]pb.Entry, len(m.Entries))
	for i, e := range m.Entries {
		entries[i] = *e
	}
	r.appendEntry(entries...)
	r.maybeCommit()
	r.broadcastAppend()
}

func (r *Raft) appendEntry(es ...pb.Entry) {
	var li = r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	li = r.RaftLog.appendEntries(es)
	r.Prs[r.id].MaybeUpdate(li)
}

func (r *Raft) committed() (uint64, bool) {
	var ln = len(r.Prs)
	if ln == 0 {
		return 0, false
	}
	var stack = make([]int, ln)
	for id, pr := range r.Prs {
		stack[int(id)-1] = int(pr.Match)
	}
	sort.Ints(stack)
	// 排序后, 取较多的值作为commit的最终结果
	var mid = ln - (ln / 2) - 1
	return uint64(stack[mid]), true
}

func (r *Raft) maybeCommit() bool {
	var commit, ok = r.committed()
	if !ok {
		return false
	}
	r.RaftLog.commitTo(commit)
	return true
}

func (r *Raft) handleVoteRequest(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
	}
	var reject bool
	if r.Vote != None && r.Vote != m.From {
		reject = true
	} else {
		r.Vote = m.From
	}
	r.send(pb.Message{From: r.id, Term: r.Term, To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: reject})
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	if r.State != StateCandidate {
		return
	}
	if m.From == r.id {
		return
	}
	r.votes[m.From] = !m.Reject

	var cnt int
	for _, vote := range r.votes {
		if vote {
			cnt++
		}
	}

	if cnt >= len(r.Prs)/2+1 {
		r.becomeLeader()
		r.broadcastAppend()
	}
}

func (r *Raft) broadcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.committed,
		})
		return
	}

	var entries = make([]pb.Entry, len(m.Entries))
	for i, e := range m.Entries {
		entries[i] = *e
	}
	r.appendEntry(entries...)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	r.sendHeartbeatResp(m.From)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
