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

func (r *Raft) half() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) send(msg pb.Message) {
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
	preLogIndex := pr.Next - 1
	var term, err = r.RaftLog.Term(preLogIndex)
	if err != nil {
		//TODO
		return false
	}
	// 要附加的Entries, 从 pr.Next 开始
	var entries = r.RaftLog.entriesSlice(preLogIndex + 1)

	var msg pb.Message
	msg.To = to
	msg.MsgType = pb.MessageType_MsgAppend
	msg.Index = preLogIndex
	msg.LogTerm = term
	msg.Commit = r.RaftLog.committed
	msg.Entries = make([]*pb.Entry, len(entries))
	for i, e := range entries {
		e := e
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

func (r *Raft) sendHeartbeatResp(to uint64, reject bool) {
	var lastIdx = r.RaftLog.LastIndex()
	var lastTerm, _ = r.RaftLog.Term(lastIdx)
	r.send(pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   lastIdx,
		LogTerm: lastTerm,
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
	if r.Term == term && r.Lead == lead && lead != None {
		// 如果Leader是None, 也需要重置
		return
	}
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
	// 附加一条Leader生成的协议
	r.appendEntry(pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	})
	// 尝试提交(?)
	r.maybeCommit()
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	for k := range r.votes {
		delete(r.votes, k)
	}
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
			r.handleAppendEntries(m)
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
				r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgHeartbeat:
			// 候选节点不应该处理心跳
			// 	r.handleHeartbeat(m)
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
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		}
	}
	return nil
}

func (r *Raft) requestElection() {
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	// 选举时, 应该发送最后一个日志的Log和Term
	var lastIdx = r.RaftLog.LastIndex()
	var lastTerm, _ = r.RaftLog.Term(lastIdx)
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			LogTerm: lastTerm,
			Index:   lastIdx,
		})
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	var pr = r.Prs[m.From]
	if pr == nil {
		return
	}

	if m.Reject {
		// 当前不匹配, 减一继续尝试匹配
		// TODO 存在一定的优化空间
		pr.Next -= 1
		r.sendAppend(m.From)
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
	r.broadcastAppend() // 注意消息的顺序
	r.maybeCommit()     // 只有一个节点时, 接收到Propose就应该直接commit
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
	var canCommit bool
	var committed = r.RaftLog.committed
	var half = r.half() //(?)
	for i := committed + 1; i <= r.RaftLog.LastIndex(); i++ {
		// 任期不同的直接跳过去
		term, _ := r.RaftLog.Term(i)
		if term != r.Term {
			continue
		}
		// 任期相同看当前匹配的节点个数
		var cnt int
		for _, p := range r.Prs {
			if p.Match >= i {
				cnt += 1
			}
		}
		if cnt < half {
			// 不满足条件直接pass
			continue
		}

		committed = i
		canCommit = true
	}
	return committed, canCommit
}

func (r *Raft) maybeCommit() bool {
	var commit, ok = r.committed()
	if !ok {
		return false
	}
	return r.RaftLog.commitTo(commit)
}

func (r *Raft) handleVoteRequest(m pb.Message) {
	// 看主Term
	if r.Term > m.Term {
		r.sendVoteResponse(m.From, true)
		return
	}

	// 重点是观察最后一条提交的日志!
	// lastIndex := r.RaftLog.LastIndex()
	// lastTerm, _ := r.RaftLog.Term(lastIndex)

	if r.isMoreUpToDateThan(m.LogTerm, m.Index) {
		// 注意: 这个函数返回true表示的时当前节点的数据比Message的数据还要新!
		if r.Term < m.Term {
			// 更新自己的周期
			r.becomeFollower(m.Term, None)
		}
		r.sendVoteResponse(m.From, true)
		return
	}

	if r.Term < m.Term {
		// 较新的投票周期, 直接通过
		// 投票并不意味着出现了Leader. 需要后续观察. 所以此时的Leader是None
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		r.sendVoteResponse(m.From, false)
		return
	}

	if r.Vote == m.From {
		r.sendVoteResponse(m.From, false)
		return
	} else if r.Vote != None {
		r.sendVoteResponse(m.From, true)
		return
	}

	r.sendVoteResponse(m.From, false)
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.Index)
		r.Vote = m.From
		return
	}
	if r.State != StateCandidate {
		return
	}
	if m.From == r.id {
		return
	}
	if r.votes == nil {
		r.votes = make(map[uint64]bool)
	}
	r.votes[m.From] = !m.Reject

	var cnt int
	var not int
	for _, vote := range r.votes {
		if vote {
			cnt++
		} else {
			not++
		}
	}

	var half = r.half()

	if cnt >= half {
		r.becomeLeader()
		r.broadcastAppend()
	} else if not >= half {
		r.becomeFollower(r.Term, None)
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
	// Term是当前任期, LogTerm是日志产生的任期

	if m.Term < r.Term {
		// 较小任期, 直接拒绝
		r.sendAppendResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)

	// 查找一下是不是已存在的日志
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		// 不匹配就直接拒绝
		r.sendAppendResponse(m.From, true)
		return
	}

	// 查找Entries, 添加到自己身上
	if len(m.Entries) > 0 {
		var appendIdx int
		for i, e := range m.Entries {
			// 添加未添加的
			if e.Index > r.RaftLog.LastIndex() {
				appendIdx = i
				break
			}
			// 删除对不上的
			term, err := r.RaftLog.Term(e.Index)
			if err == nil && term != e.Term {
				appendIdx = i
				r.RaftLog.RemoveEntriesAfter(e.Index)
				break
			}
			appendIdx = i
		}

		if m.Entries[appendIdx].Index > r.RaftLog.LastIndex() {
			for _, e := range m.Entries[appendIdx:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
		}
	}

	if m.Commit > r.RaftLog.committed {
		//But why? 文档这么说的, 别问我
		var lastEntryIndex = m.Index
		if len(m.Entries) > 0 {
			lastEntryIndex = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastEntryIndex)
	}

	r.sendAppendResponse(m.From, false)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	// Leader 在接收到Follower的心跳回复时, 需要判断是否要附加日志到Follower上
	if r.isMoreUpToDateThan(m.LogTerm, m.Index) {
		r.sendAppend(m.From)
	}
}

func (r *Raft) isMoreUpToDateThan(logTerm, logIndex uint64) bool {
	var lastIndex = r.RaftLog.LastIndex()
	var lastTerm, _ = r.RaftLog.Term(lastIndex)
	// 日志的周期小于当前周期, 或者相同的情况下, 当前Index > 日志的Index
	return logTerm < lastTerm || (logTerm == lastTerm && lastIndex > logIndex)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term >= r.Term {
	} else {
		// 小于当前Term的无视即可
		r.sendHeartbeatResp(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)

	// 对比Term
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendHeartbeatResp(m.From, true)
		return
	}

	// 保留当前可达的Commit位置
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}

	r.sendHeartbeatResp(m.From, false)
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	})
}

func (r *Raft) sendVoteResponse(to uint64, reject bool) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		Term:    r.Term,
		To:      to,
		Reject:  reject,
	})
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
