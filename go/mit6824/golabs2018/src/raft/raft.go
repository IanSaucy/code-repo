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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "labgob"


const mylogEnabled = true
func mylog(a ...interface{}) (n int, err error) {
	if mylogEnabled {
		n, err = fmt.Println( a...)
	}
	return
}

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
}

// 3种状态
const STATE_LEADER int = 1
const STATE_FOLLOWER int = 2
const STATE_CANDIDATE int = 3
const STATE_KILLED int = -1 // 表示raft终止了

// 无效的候选人id
const NULL_CANDIDATE_ID int = -1

// 消息
const MSG_APPEND_ENTRIES = 1
const MSG_REQUEST_VOTE = 2

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


	// 开始新状态之前，必须保证之前开始的旧状态的代码不起作用
	// 每次开始新状态时，分配一个逻辑时钟，所有的相关代码必须只能在逻辑时钟相同的情况起作用
	logicalClock int
	
	// AppendEntries RPC计数
	appendEntriesCount int
	// RequestVote RPC计数
	requestVoteCount int
	
	// 当前状态
	state int
	// 当前任期
	currentTerm int
	// 投票的候选人id
	votedFor int
}

func (rf *Raft)getLogicalClock() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.logicalClock
}
func (rf *Raft)getAppendEntriesCount() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.appendEntriesCount
}
func (rf *Raft)setAppendEntriesCount(v int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.appendEntriesCount = v
}
func (rf *Raft)getRequestVoteCount() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.requestVoteCount
}
func (rf *Raft)setRequestVoteCount(v int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.requestVoteCount = v
}
func (rf *Raft)getState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}
func (rf *Raft)setState(v int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = v
}
func (rf *Raft)getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}
func (rf *Raft)setVotedFor(v int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = v
}
func (rf *Raft)voteForSelf() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = rf.me
}
func (rf *Raft)incCurrentTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
}
func (rf *Raft)addCurrentTerm(v int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += v
}
func (rf *Raft)getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}
func stateToString(state int) string {
	if state == STATE_FOLLOWER {
		return "Follower"
	} else if state == STATE_CANDIDATE {
		return "Candidate"
	} else if state == STATE_LEADER {
		return "Leader"
	} else if state == STATE_KILLED {
		return "Killed"
	} else {
		return fmt.Sprintf("其他%d", state)
	}
}
func (rf *Raft)getStateDescription() string {
	return fmt.Sprintf("Raft[%d]任期[%d]状态[%s]逻辑时钟[%d]", rf.me, rf.getCurrentTerm(),  stateToString(rf.getState()), rf.getLogicalClock())
}
func (rf *Raft)getStateDescription2() string {
	return fmt.Sprintf("Raft[%d]任期[%d]状态[%s]逻辑时钟[%d]", rf.me, rf.currentTerm,  stateToString(rf.state), rf.logicalClock)
}
// 获取随机的选举超时时间，单位毫秒
func (rf *Raft)getElectionTimeout() time.Duration {
	const MIN_ELECTION_TIMEOUT = 500
	const MAX_ELECTION_TIMEOUT = 1000
	ms := MIN_ELECTION_TIMEOUT + (rand.Int63() % (MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT))
	return time.Duration(ms) * time.Millisecond
}
// 获取Leader发送心跳信息的时间，单位毫秒
func (rf *Raft)getHeartbeatTimeout() time.Duration {
	const HEARTBEAT_TIMEOUT = 80
	ms := HEARTBEAT_TIMEOUT
	return time.Duration(ms) * time.Millisecond	
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.getCurrentTerm()
	isleader = rf.getState() == STATE_LEADER
	
	return term, isleader
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
}


//
// restore previously persisted state.
//
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // 候选人的任期
	CandidateId int  // 候选人的id
	LastLogIndex int // 候选人最新日志的索引
	LastLogTerm int // 候选人最新日志的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	Term int // 当前任期，用于候选人更新自己的任期
	VoteGranted bool // true表示接收了投票
}

func (rf *Raft)checkRequestVoteTerm(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		mylog(rf.getStateDescription2(), "，发现更高Raft[", args.CandidateId, "]任期[", args.Term, "]，切换到Follower")
		go rf.startState(STATE_FOLLOWER)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	mylog(rf.getStateDescription(), "收到RequestVote RPC")
	
	// Your code here (2A, 2B).

	// All Servers : If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	rf.checkRequestVoteTerm(args)
	
	// Followers : Respond to RPCs from candidates and leaders
	if rf.getState() == STATE_FOLLOWER {
		// 1. Reply false if term < currentTerm (§5.1)
		if args.Term < rf.getCurrentTerm() {
			reply.Term = rf.getCurrentTerm()
			reply.VoteGranted = false
			return
		}
		
		// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if rf.votedFor == NULL_CANDIDATE_ID || rf.votedFor == args.CandidateId {
			reply.Term = rf.getCurrentTerm()
			reply.VoteGranted = true
			return	
		}
	}

	reply.Term = rf.getCurrentTerm()
	reply.VoteGranted = false
	return
}

type LogEntry struct {
	Term int
	Index int
}

type AppendEntriesArgs struct {
	Term int // Leader的任期
	LeaderId int // Leader的id
	PrevLogIndex int // Entries[]前面的一个日志索引
	PrevLogTerm int // Entries[]前面的一个日志任期
	Entries []LogEntry
	LeaderCommit int // Leader提交的日志索引
}

type AppendEntriesReply struct {
	Term int // 当前任期
	Success bool // true表示Follower匹配
}

func (rf *Raft)AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// All Servers : If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	
	// 1. Reply false if term < currentTerm (§5.1)
	
	
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	
	// 4. Append any new entries not already in the log
	
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
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

func (rf *Raft)sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.

	rf.setState(STATE_KILLED)
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
	
	rf.logicalClock = 0
	rf.appendEntriesCount = 0
	rf.requestVoteCount = 0
	rf.state = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = NULL_CANDIDATE_ID

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startState(STATE_FOLLOWER)

	return rf
}

func (rf *Raft)internalStartNewState(state int) {
	rf.state = state
	rf.logicalClock++
	
	if state == STATE_FOLLOWER {
		go rf.startFollower(rf.logicalClock)
	} else if state == STATE_CANDIDATE {
		go rf.startCandidate(rf.logicalClock)
	} else if state == STATE_LEADER {
		go rf.startLeader(rf.logicalClock)
	}
}
func (rf *Raft)startState(state int) {
	mylog(rf.getStateDescription(), "，开始状态--->", stateToString(state))
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.internalStartNewState(state)
}
func (rf *Raft)startStateByLogicalClock(state int, logicalClock int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (logicalClock == rf.logicalClock) {
		rf.internalStartNewState(state)
	} else {
		mylog(rf.getStateDescription2(), ",不能改变状态，因为logicalClock:", logicalClock)
	}
}

func (rf *Raft)startFollower(logicalClock int) {
	rf.setAppendEntriesCount(0)
	rf.setRequestVoteCount(0)
	rf.setVotedFor(NULL_CANDIDATE_ID)

	duration := rf.getElectionTimeout()
	electionTimeoutTimer := time.NewTimer(duration)
	<- electionTimeoutTimer.C

	aeCount := rf.getAppendEntriesCount()
	rvCount := rf.getRequestVoteCount()
	votedFor := rf.getVotedFor()
	mylog(rf.getStateDescription(), "在", duration, "毫秒之后选举超时,AppendEntries RPC个数:", aeCount, "，RequestVote RPC个数:", rvCount, "，VotedFor:", votedFor)
	
	if aeCount == 0 && votedFor == NULL_CANDIDATE_ID {
		mylog(rf.getStateDescription(), "切换到", stateToString(STATE_CANDIDATE))
		go rf.startStateByLogicalClock(STATE_CANDIDATE, logicalClock)
	} else {
		mylog(rf.getStateDescription(), "继续保持", stateToString(STATE_FOLLOWER))
		go rf.startStateByLogicalClock(STATE_FOLLOWER, logicalClock)	
	}	
}

func (rf *Raft)startCandidate(logicalClock int) {
	rf.incCurrentTerm() // 	Increment currentTerm
	rf.voteForSelf() //  Vote for self

	// Reset election timer
	duration := rf.getElectionTimeout() 
	electionTimeoutTimer := time.NewTimer(duration)

	voteChan := make(chan int, len(rf.peers))
	voteArgs := RequestVoteArgs{
		Term : rf.getCurrentTerm(),
		CandidateId : rf.getVotedFor(),
		LastLogIndex : 0,
		LastLogTerm : 0}
	var replyList []RequestVoteReply

	for i := 0; i < len(rf.peers); i++ {
		replyList = append(replyList, RequestVoteReply{})
	}
	
	// Send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		go func(server int) {
			mylog("开始向服务器", server, "发送RequestVote RPC")
			ok := rf.sendRequestVote(server, &voteArgs, &replyList[server])
			voteChan <- server
			mylog("结束向服务器", server, "发送RequestVote RPC,", ok)
			
		}(i)
	}

	vote := 0
	voteCount := 0
	
	for done := false; !done; {
		select {
		case <- electionTimeoutTimer.C:
			// 超时了
			done = true
			rf.afterCandidateElectionTimeout(logicalClock, duration)
		case vote = <- voteChan:
			// 第N个RPC返回了
			if replyList[vote].VoteGranted {
				voteCount++
				majority := len(rf.peers) / 2 + 1
				if voteCount >= majority {
					// 收到多数派投票
					done = true
					mylog("多数派个数:", majority)
					rf.afterCandidateRecvMajorityVote(logicalClock, voteCount, len(rf.peers))
				}
			}
		}
	}
}
func (rf *Raft)afterCandidateElectionTimeout(logicalClock int, duration time.Duration) {
	aeCount := rf.getAppendEntriesCount()
	rvCount := rf.getRequestVoteCount()
	votedFor := rf.getVotedFor()
	mylog(rf.getStateDescription(), "在", duration, "毫秒之后选举超时,AppendEntries RPC个数:", aeCount, "，RequestVote RPC个数:", rvCount, "，VotedFor:", votedFor)

	mylog(rf.getStateDescription(), "继续保持", stateToString(STATE_CANDIDATE))
	go rf.startStateByLogicalClock(STATE_CANDIDATE, logicalClock)
}
func (rf *Raft)afterCandidateRecvAppendEntriesRPC(logicalClock int) {
	aeCount := rf.getAppendEntriesCount()
	rvCount := rf.getRequestVoteCount()
	votedFor := rf.getVotedFor()
	mylog(rf.getStateDescription(), "收到AppendEntries RPC,AppendEntries RPC个数:", aeCount, "，RequestVote RPC个数:", rvCount, "，VotedFor:", votedFor)

	mylog(rf.getStateDescription(), "切换到", stateToString(STATE_FOLLOWER))
	go rf.startStateByLogicalClock(STATE_FOLLOWER, logicalClock)
}
func (rf *Raft)afterCandidateRecvMajorityVote(logicalClock int, voteCount int, total int) {
	aeCount := rf.getAppendEntriesCount()
	rvCount := rf.getRequestVoteCount()
	votedFor := rf.getVotedFor()
	mylog(rf.getStateDescription(), "收到多数派投票", voteCount, "/", total, ",AppendEntries RPC个数:", aeCount, "，RequestVote RPC个数:", rvCount, "，VotedFor:", votedFor)

	mylog(rf.getStateDescription(), "切换到", stateToString(STATE_LEADER))
	go rf.startStateByLogicalClock(STATE_LEADER, logicalClock)
}

func (rf *Raft)startLeader(logicalClock int) {
	// Upon election: send initial empty AppendEntries RPCs
	// (heartbeat) to each server; repeat during idle periods to
	// prevent election timeouts (§5.2)

	go rf.startLeaderSendHeartbeatTimer()

}
func (rf *Raft)startLeaderSendHeartbeatTimer() {	
	args := AppendEntriesArgs{
		Term : rf.getCurrentTerm(),
		LeaderId : rf.me,
		PrevLogIndex : 0,
		PrevLogTerm : 0,
		Entries : []LogEntry{},
		LeaderCommit : 0,
	}
	replyList := []AppendEntriesReply{}

	for i := 0; i < len(rf.peers); i++ {
		replyList = append(replyList, AppendEntriesReply{})
	}
	
	for i := 0; i < len(rf.peers); i++ {
		go func(server int) {
			//mylog("开始向服务器", server, "发送心跳信息AppendEntries RPC")
			rf.sendAppendEntries(server, &args, &replyList[server])
			//mylog("结束向服务器", server, "发送心跳信息AppendEntries RPC,", ok)
		
		}(i)
	}
	
	duration := rf.getHeartbeatTimeout()
	timer := time.NewTimer(duration)
	<- timer.C

	//aeCount := rf.getAppendEntriesCount()
	//rvCount := rf.getRequestVoteCount()
	//votedFor := rf.getVotedFor()
	//mylog(rf.getStateDescription(), "在", duration, "毫秒之后心跳超时,AppendEntries RPC个数:", aeCount, "，RequestVote RPC个数:", rvCount, "，VotedFor:", votedFor)

	go rf.startLeaderSendHeartbeatTimer()
}
