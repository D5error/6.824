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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool        // 如果日志条目已经提交，则设置为 true
	Command      interface{} // 已提交的命令
	CommandIndex int         // 日志条目的索引

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// D5 for 2A
type LogEntries struct {
	Command interface{}
	Term    int
}

// D5 for 2A
type Role int // 领导者、候选者或跟随者
const (
	Follower Role = iota
	Candidate
	Leader
)

// D5 for 2A
const (
	ElectionTimeout  = time.Millisecond * 200
	HeartBeatTimeout = time.Millisecond * 100
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// D5 for 2A
	currentTerm    int  // 当前任期
	votedFor       int  // 投票给候选者peers[votedFor]
	role           Role // 领导者、候选者或跟随者
	log            []LogEntries
	electionTimer  *time.Timer   // 选举计时器
	heartbeatTimer *time.Timer   // 心跳计时器
	killCh         chan struct{} // 当节点停止时用于通知goroutine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	// D5 for 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.role == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// D5 for 2A
	Term         int // 候选人的任期
	CandidateId  int // 候选人的id
	LastLogIndex int // 候选人最后一条日志条目的索引
	LastLogTerm  int // 最后一条日志条目所处的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	// D5 for 2A
	Term        int  // 投票人的当前任期，给候选人更新任期用
	VoteGranted bool // 是否同意投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// D5 for 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	reply.Term = rf.currentTerm // 提供投票者当前任期
	reply.VoteGranted = false

	rf.resetElectionTimer() // 更新选举计时器

	// 任期更大，或任期相同时日志更多
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return
	}

	if args.Term < rf.currentTerm {
		return
	} else if args.Term == rf.currentTerm {
		if rf.role == Leader {
			return
		} else if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			return
		} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			return
		}
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.changeToFollower()

	reply.VoteGranted = true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// D5 for 2A
type AppendEntriesArgs struct {
	Term         int          // 领导者的任期
	LeaderId     int          // 领导者的Id
	PrevLogIndex int          // 最后一个日志条目的索引
	PrevLogTerm  int          // 最后一个日志条目的任期
	Entries      []LogEntries // 传输的日志
	LeaderCommit int          // 领导者认为可以提交的最新日志条目的索引
}

// D5 for 2A
type AppendEntriesReply struct {
	Term    int  // 跟随者当前任期
	Success bool // 是否成功匹配日志一致性
}

// D5 for 2A
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.changeToFollower() // 候选者变为跟随者
		rf.votedFor = -1
		rf.currentTerm = args.Term // 更新任期
	}
	rf.resetElectionTimer() // 接收到心跳，重置计时器
}

// D5 for 2A
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	// D5 for 2A
	close(rf.killCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// D5 for 2A
func (rf *Raft) election() {
	rf.mu.Lock()

	// 当前已经是领导者，不用再次发布选举
	if rf.role == Leader {
		rf.mu.Unlock()
		return
	}

	rf.resetElectionTimer() // 重置计时器

	rf.role = Candidate // 变为跟随者
	rf.currentTerm++    // 新的选举，增加任期
	rf.votedFor = rf.me // 我为自己投票

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	grantedCount := 1 // 通过的数量
	votedCount := 1   // 记录票数

	votesCh := make(chan bool, len(rf.peers)) // 用来投票

	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(ch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			ch <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.changeToFollower()
					rf.votedFor = -1
					rf.resetElectionTimer()
				}
				rf.mu.Unlock()
			}
		}(votesCh, index)
	}

	for {
		r := <-votesCh
		votedCount += 1
		if r == true {
			grantedCount += 1
		}
		if votedCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || votedCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		return
	}

	rf.mu.Lock()
	if rf.currentTerm == args.Term && rf.role == Candidate {
		rf.changeToLeader()
	}

	rf.mu.Unlock()
}

// D5 for 2A
func randomizeElectionTimeout() time.Duration {
	return ElectionTimeout + (time.Duration(rand.Intn(200)) * time.Millisecond)
}

// D5 for 2A
func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randomizeElectionTimeout())
}

// D5 for 2A
func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(HeartBeatTimeout)
}

// D5 for 2A
func (rf *Raft) lastLogTermIndex() (int, int) {
	return rf.log[len(rf.log)-1].Term, len(rf.log) - 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// D5 for 2A
		select {
		case <-rf.killCh: // 停止运行
			return
		case <-rf.electionTimer.C: // 倒计时超时则发布选举
			go rf.election()
		}
	}
}

// D5 for 2A
func (rf *Raft) heartbeatTicker(peers []*labrpc.ClientEnd) {
	for {
		select {
		case <-rf.killCh:
			return
		case <-rf.heartbeatTimer.C:
			for index := range peers {
				if rf.me == index {
					continue
				} else {
					rf.resetHeartbeatTimer()
					go rf.heartbeat(index)
				}
			}
		}
	}

}

// D5 for 2A
func (rf *Raft) heartbeat(server int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term: rf.currentTerm,
	}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(server, &args, &reply)

	if reply.Term > args.Term {
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.changeToFollower()
			rf.votedFor = -1
			rf.resetElectionTimer()
		}
		rf.mu.Unlock()
	}
}

// D5 for 2A
func (rf *Raft) changeToLeader() {
	rf.role = Leader
}

// D5 for 2A
func (rf *Raft) changeToCandidate() {
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.currentTerm += 1
}

// D5 for 2A
func (rf *Raft) changeToFollower() {
	rf.role = Follower
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// D5 for 2A
	rf.currentTerm = 0 // 任期初始化为0
	rf.votedFor = -1 // 还没投票
	rf.killCh = make(chan struct{})
	rf.log = make([]LogEntries, 1)
	rf.changeToFollower()// 初始化为跟随者
	rf.electionTimer = time.NewTimer(randomizeElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartBeatTimeout)
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	// start ticker goroutine to start elections
	go rf.ticker()

	// D5 for 2A
	go rf.heartbeatTicker(peers)
	return rf
}
