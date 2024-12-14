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
	// "fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	// "github.com/cosiner/argv"
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
type LogEntry struct {
	Command interface{}
	Term    int
}

// D5 for 2A
func (rf *Raft) lastLogTermIndex() (int, int) {
	return rf.logs[len(rf.logs)-1].Term, len(rf.logs) - 1
}

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
	currentTerm     int        // 当前任期
	votedFor        int        // 投票给候选者
	isLeader        bool       //是否为领导者
	logs            []LogEntry // 日志
	hasLeader       bool       // 是否有领导者
	resetTickerChan chan bool  // 用于重置计时器
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	// D5 for 2A
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()

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
func (voter *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// D5 for 2A
	voter.mu.Lock()
	defer voter.mu.Unlock()
	// fmt.Printf("term=%d: %d受到来自%d的票\n", voter.currentTerm, voter.me, args.CandidateId)

	reply.Term = voter.currentTerm // 提供投票者当前任期

	// receiver implementation 1
	if args.Term <= voter.currentTerm {
		reply.VoteGranted = false
		return

	} else if args.Term > voter.currentTerm {
		voter.currentTerm = args.Term // 更新投票者的term

		// zgh for 2A
		voter.votedFor = -1
		voter.isLeader = false
	}

	// receiver implementation 2
	voter_lastLogTerm, voter_lastLogIndex := voter.lastLogTermIndex()
	if (voter.votedFor == -1 || voter.votedFor == args.CandidateId) && (args.LastLogTerm == voter_lastLogTerm && args.LastLogIndex >= voter_lastLogIndex) {
		voter.resetTickerChan <- true // 重置计时器
		voter.votedFor = args.CandidateId
		voter.isLeader = false
		voter.hasLeader = true
		reply.VoteGranted = true
		// fmt.Printf("term=%d: %d投票给%d\n", voter.currentTerm, voter.me, voter.votedFor)
		return

	} else {
		reply.VoteGranted = false
		return
	}
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
	Term         int        // 领导者的任期
	LeaderId     int        // 领导者的Id
	PrevLogIndex int        // 最后一个日志条目的索引
	PrevLogTerm  int        // 最后一个日志条目的任期
	Entries      []LogEntry // 传输的日志
	LeaderCommit int        // 领导者认为可以提交的最新日志条目的索引
}

// D5 for 2A
type AppendEntriesReply struct {
	Term    int  // 跟随者当前任期
	Success bool // 是否成功匹配日志一致性
}

// D5 for 2A
func (follower *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	follower.mu.Lock()
	defer follower.mu.Unlock()

	reply.Term = follower.currentTerm

	if args.Term < follower.currentTerm {
		reply.Success = false
		return
	}

	// fmt.Printf("term=%d: %d <--- %d\n", follower.currentTerm, follower.me, args.LeaderId)

	follower.resetTickerChan <- true
	follower.isLeader = false // 变为跟随者
	follower.hasLeader = true
	follower.votedFor = -1
	follower.currentTerm = args.Term // 更新任期

	reply.Success = true
}

// D5 for 2A
func (leader *Raft) sendAppendEntries(follower int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := leader.peers[follower].Call("Raft.AppendEntries", args, reply)
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
	rf = nil // 防止bug发生
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// D5 for 2A
		timeout := 150*time.Millisecond + time.Duration(rand.Int63()%250)*time.Millisecond
		select {
		case <-rf.resetTickerChan:
			// fmt.Printf("term=%d, %d reset ticker\n", rf.currentTerm, rf.me)
			continue // 接收到重置计时信号

		case <-time.After(timeout):
			rf.mu.Lock()
			rf.hasLeader = false
			rf.votedFor = -1
			// fmt.Printf("term=%d: %d timeout\n", rf.currentTerm, rf.me)
			rf.mu.Unlock()
			// 计时器超时，进入阻塞状态，再次收到重置计时信号则重新开始计时
			go rf.election()
			select {
			case <-rf.resetTickerChan:
				continue
			}
		}
	}
}

// D5 for 2A
func (leader *Raft) heartBeater() {
	leader.mu.Lock()
	if !leader.isLeader || leader.hasLeader {
		leader.mu.Unlock()
		return
	}

	leader_lastLogTerm, leader_lastLogIndex := leader.lastLogTermIndex()
	args := AppendEntriesArgs{
		Term:         leader.currentTerm,
		LeaderId:     leader.me,
		PrevLogIndex: leader_lastLogIndex,
		PrevLogTerm:  leader_lastLogTerm,
	}
	leader.mu.Unlock()

	for !leader.killed() {
		leader.mu.Lock()
		if !leader.isLeader || leader.hasLeader {
			leader.mu.Unlock()
			return
		}
		leader.mu.Unlock()

		for follower := range leader.peers {
			if follower == leader.me {
				continue
			}
			reply := AppendEntriesReply{}
			leader.sendAppendEntries(follower, &args, &reply)

			// 发起心跳时，发现有任期更大的，取消发起心跳，更新任期
			leader.mu.Lock()
			// fmt.Printf("term=%d: %d -> %d(%t)\n", leader.currentTerm, leader.me, follower, ok)
			if reply.Term > leader.currentTerm {
				leader.currentTerm = reply.Term
				leader.isLeader = false
				leader.hasLeader = false
				leader.votedFor = -1
				// fmt.Printf("term=%d: find a bigger term=%d from %d\n", leader.currentTerm, reply.Term, follower)
				leader.mu.Unlock()
				return
			}
			if !leader.isLeader || leader.hasLeader {
				leader.mu.Unlock()
				return
			}
			leader.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// D5 for 2A
func (rf *Raft) election() {
	voteChan := make(chan bool, len(rf.peers))
	voteWinChan := make(chan bool) // 有多数同意票同意

	// 函数：检查票数是否达到大多数
	checkVotesCount := func() {
		// 统计票数
		count := 1 // 自己给自己投1票

		for {
			vote := <-voteChan
			if vote {
				count += 1
			}
			// 半数同意票
			if count*2 > len(rf.peers) {
				voteWinChan <- true
				return
			}
		}
	}

	// 函数：发送票给peers
	sendVoteToPeers := func() {
		rf.mu.Lock()
		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		rf.mu.Unlock()

		for id := range rf.peers {
			if id == rf.me {
				continue
			}
			//  发送票给id
			go func(server int) {
				reply := RequestVoteReply{}
				rf.sendRequestVote(server, &args, &reply)
				voteChan <- reply.VoteGranted // 接收到票

				// 发起投票时，发现有任期更大的投票者，取消发起投票，更新任期
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.isLeader = false
					rf.hasLeader = false
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}(id)
		}
	}

	rf.mu.Lock()
	if rf.isLeader || rf.hasLeader { // 已经是leader，或已经有领导者
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++    // 新的选举要增加任期
	rf.votedFor = rf.me // 先为自己投票
	// fmt.Printf("term=%d: %d try to election\n", rf.currentTerm, rf.me)
	rf.mu.Unlock()

	sendVoteToPeers()    // 发送票给peers
	go checkVotesCount() //  检查是否有多数同意票

	select {
	// 有多数同意票同意
	case <-voteWinChan:
		rf.mu.Lock()
		rf.isLeader = true
		rf.votedFor = -1
		rf.hasLeader = false
		// fmt.Printf("\nterm=%d: %d成为领导者\n", rf.currentTerm, rf.me)
		rf.mu.Unlock()
		go rf.heartBeater() // 向跟随者发送心跳

	// 收到其它leader的心跳，不要再进行选举
	case <-rf.resetTickerChan:
		rf.mu.Lock()
		// fmt.Printf("%d received heartbeat, aborting election.\n", rf.me)
		rf.isLeader = false
		rf.votedFor = -1
		rf.hasLeader = true
		rf.mu.Unlock()

	case <-time.After(150*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond):
	// case <-time.After(200*time.Millisecond + time.Duration(rand.Int63()%100)*time.Millisecond):
		rf.mu.Lock()
		rf.isLeader = false
		rf.hasLeader = false
		rf.votedFor = -1
		rf.resetTickerChan <- true
		rf.mu.Unlock()
	}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).

	// D5 for 2A
	rf.currentTerm = 0 // 任期初始化为0
	rf.votedFor = -1   // 还没投票
	rf.logs = make([]LogEntry, 1)
	rf.isLeader = false // 初始化为跟随者
	rf.resetTickerChan = make(chan bool)
	rf.hasLeader = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
