package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

const (
	heartbeatInterval  = 100 * time.Millisecond
	minElectionTimeout = 3 * heartbeatInterval
	maxElectionTimeout = 5 * heartbeatInterval
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Snapshot     []byte
}

type Log struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	isLeader bool

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	votes           int

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	logs        []Log

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndexes  []int
	matchIndexes []int

	startIndex   int
	Maxraftstate int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte {
	buffer := new(bytes.Buffer)
	e := labgob.NewEncoder(buffer)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.startIndex)
	return buffer.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	buffer := bytes.NewBuffer(data)
	d := labgob.NewDecoder(buffer)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
	d.Decode(&rf.startIndex)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	lastLogIndex += rf.startIndex

	rf.updateTerm(args.Term)
	reply.VoteGranted = args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.electionTimer.Reset(nextTimeout())
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	rf.updateTerm(args.Term)
	rf.electionTimer.Reset(nextTimeout())

	if args.PrevLogIndex >= len(rf.logs)+rf.startIndex {
		reply.ConflictIndex = len(rf.logs) + rf.startIndex
	} else if index := args.PrevLogIndex - rf.startIndex; index >= 0 && rf.logs[index].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[index].Term
		for index >= 0 && rf.logs[index].Term == reply.ConflictTerm {
			index--
		}
		reply.ConflictIndex = index + 1 + rf.startIndex
	} else {
		reply.Success = true
		i, j := 0, args.PrevLogIndex+1-rf.startIndex
		for ; i < len(args.Entries) && j < len(rf.logs); i, j = i+1, j+1 {
			if j >= 0 && rf.logs[j].Term != args.Entries[i].Term {
				break
			}
		}
		if i < len(args.Entries) {
			rf.logs = append(rf.logs[:j], args.Entries[i:]...)
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.setCommitIndex(min(args.LeaderCommit, len(rf.logs)-1+rf.startIndex))
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.updateTerm(args.Term)
	rf.electionTimer.Reset(nextTimeout())

	if rf.startIndex >= args.LastIncludedIndex {
		return
	}

	if i := args.LastIncludedIndex - rf.startIndex; i < len(rf.logs) &&
		rf.logs[i].Term == args.LastIncludedTerm {
		if rf.lastApplied < args.LastIncludedIndex {
			rf.commitIndex = args.LastIncludedIndex
			rf.lastApplied = args.LastIncludedIndex
			rf.applyCh <- ApplyMsg{Snapshot: args.Data}
		}
		rf.saveStateAndSnapshot(args.LastIncludedIndex, args.Data)
	} else {
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
		rf.applyCh <- ApplyMsg{Snapshot: args.Data}
		rf.startIndex = args.LastIncludedIndex
		rf.logs = []Log{{Term: args.LastIncludedTerm}}
		rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.logs) + rf.startIndex

	// Your code here (2B).
	if rf.isLeader {
		rf.logs = append(rf.logs, Log{command, rf.currentTerm})
		rf.persist()
		rf.matchIndexes[rf.me] = index
		rf.nextIndexes[rf.me] = index + 1
		rf.sendHeartbeats()
	}
	return index, rf.currentTerm, rf.isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.votedFor = -1
	rf.logs = make([]Log, 1) // log index starts at 1

	rf.Maxraftstate = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.startIndex
	rf.lastApplied = rf.startIndex

	rf.electionTimer = time.NewTimer(nextTimeout())
	rf.heartbeatTicker = time.NewTicker(heartbeatInterval)
	rf.heartbeatTicker.Stop()
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.startElection()
			case <-rf.heartbeatTicker.C:
				rf.mu.Lock()
				rf.sendHeartbeats()
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}

func (rf *Raft) sendHeartbeats() {
	for i := range rf.peers {
		if i != rf.me {
			prevIndex := rf.nextIndexes[i] - 1
			if prevIndex-rf.startIndex < 0 {
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.startIndex,
					LastIncludedTerm:  rf.logs[0].Term,
					Data:              rf.persister.ReadSnapshot(),
				}
				reply := &InstallSnapshotReply{}
				go func(i int) {
					if rf.sendInstallSnapshot(i, args, reply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if args.Term != rf.currentTerm {
							return
						}
						if rf.updateTerm(reply.Term) {
							rf.persist()
							return
						}
						if rf.matchIndexes[i] < args.LastIncludedIndex {
							rf.matchIndexes[i] = args.LastIncludedIndex
							rf.nextIndexes[i] = args.LastIncludedIndex + 1
						}
					}
				}(i)
			} else {
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  rf.logs[prevIndex-rf.startIndex].Term,
					// make a deep copy to avoid overridden by new leaders
					Entries:      append(make([]Log, 0), rf.logs[prevIndex+1-rf.startIndex:]...),
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				go func(i int) {
					if rf.sendAppendEntries(i, args, reply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if args.Term != rf.currentTerm {
							return
						}
						if rf.updateTerm(reply.Term) {
							rf.persist()
							return
						}

						if reply.Success {
							rf.matchIndexes[i] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndexes[i] = rf.matchIndexes[i] + 1

							a := make([]int, len(rf.matchIndexes))
							copy(a, rf.matchIndexes)
							m := quickSelect(a, 0, len(a)-1, (len(a)-1)/2)
							if m > rf.commitIndex && rf.logs[m-rf.startIndex].Term == rf.currentTerm {
								rf.setCommitIndex(m)
							}
						} else {
							rf.nextIndexes[i] = reply.ConflictIndex
							if reply.ConflictTerm != 0 {
								for j := args.PrevLogIndex - rf.startIndex; j > 0; j-- {
									if rf.logs[j].Term == reply.ConflictTerm {
										rf.nextIndexes[i] = j + 1 + rf.startIndex
										break
									}
								}
							}
						}
					}
				}(i)
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	rf.votes = 1
	rf.electionTimer.Reset(nextTimeout())

	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	lastLogIndex += rf.startIndex
	for i := range rf.peers {
		if i != rf.me {
			args := &RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
			reply := &RequestVoteReply{}
			go func(i int) {
				if rf.sendRequestVote(i, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if args.Term != rf.currentTerm {
						return
					}
					if rf.updateTerm(reply.Term) {
						rf.persist()
						return
					}

					if reply.VoteGranted {
						rf.votes++
						if !rf.isLeader && rf.votes > len(rf.peers)/2 {
							rf.isLeader = true
							rf.matchIndexes = make([]int, len(rf.peers))
							rf.nextIndexes = make([]int, len(rf.peers))
							for i := range rf.nextIndexes {
								rf.nextIndexes[i] = len(rf.logs) + rf.startIndex
							}
							rf.electionTimer.Stop()
							rf.heartbeatTicker.Reset(heartbeatInterval)
							rf.sendHeartbeats()
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) updateTerm(term int) bool {
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = -1
		if rf.isLeader {
			rf.isLeader = false
			rf.heartbeatTicker.Stop()
			rf.electionTimer.Reset(nextTimeout())
		}
		return true
	}
	return false
}

func (rf *Raft) setCommitIndex(index int) {
	rf.commitIndex = index
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		if rf.lastApplied-rf.startIndex >= 0 {
			rf.applyCh <- ApplyMsg{true, rf.logs[rf.lastApplied-rf.startIndex].Command, rf.lastApplied, nil}
		}
	}
	if rf.Maxraftstate > -1 && rf.persister.RaftStateSize() > rf.Maxraftstate {
		rf.applyCh <- ApplyMsg{CommandIndex: index}
		rf.saveStateAndSnapshot(index, (<-rf.applyCh).Snapshot)
	}
}

func (rf *Raft) saveStateAndSnapshot(startIndex int, snapshot []byte) {
	if startIndex > rf.startIndex {
		// copy the remaining log entries to a new array so that the old array can be garbage collected
		rf.logs = append(make([]Log, 0), rf.logs[startIndex-rf.startIndex:]...)
		rf.startIndex = startIndex
		rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	}
}

func nextTimeout() time.Duration {
	return time.Duration(rand.Int63n(int64(maxElectionTimeout-minElectionTimeout))) + minElectionTimeout
}

func quickSelect(a []int, l, r, k int) int {
	i, j := l, l+1
	for j <= r {
		if a[j] < a[l] {
			i++
			a[i], a[j] = a[j], a[i]
		}
		j++
	}
	a[l], a[i] = a[i], a[l]
	if i == k {
		return a[i]
	} else if i < k {
		return quickSelect(a, i+1, r, k)
	} else {
		return quickSelect(a, l, i-1, k)
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
