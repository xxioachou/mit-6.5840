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

	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command 	interface{}
	Term		int
}

const (
	FOLLOWER	= iota
	CANDIDATE
	LEADER
)

const UNVOTE = -1
const MINTIMEOUT = 200
const MAXTIMEOUT = 300
const HEARTBEATTIMEOUT = 100

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 当前服务器的 term
	currentTerm			int
	// 当前服务器的身份 (follower,candidate,leader)
	identity 			int
	// 当前服务器给哪个候选人投票
	voteFor				int
	// 当前服务器得到的票数
	votes				int
	// 当前服务器的计时器过去了多少时间(ms)
	passedMs			int
	// 当前服务器发生 timeout 需要多少时长(ms)
	timeoutNeedMs		int

	// // 当前服务器维护的 logs
	// logs			[]LogEntry

}

// 返回 [l, r] 之间的随机数
func getRand(l, r int) int {
	len := int64(r - l + 1)
	return l + int(rand.Int63() % len)
}

func (rf *Raft) String() string {
	var str strings.Builder
	str.Write([]byte(fmt.Sprintf("server: %d", rf.me)))
	str.Write([]byte(fmt.Sprintf(", currentTerm: %d", rf.currentTerm)))
	var identity string
	if rf.identity == LEADER {
		identity = "leader"
	} else if rf.identity == CANDIDATE {
		identity = "candidate"
	} else if rf.identity == FOLLOWER {
		identity = "follower"
	}
	str.Write([]byte(fmt.Sprintf(", identity: %s", identity)))
	str.Write([]byte(fmt.Sprintf(", voteFor: %d", rf.voteFor)))
	str.Write([]byte(fmt.Sprintf(", votes: %d", rf.votes)))
	str.Write([]byte(fmt.Sprintf(", passedMs: %d", rf.passedMs)))
	str.Write([]byte(fmt.Sprintf(", timeoutNeedMs: %d\n", rf.timeoutNeedMs)))
	return str.String()
}

// 辅助函数(调用者加锁):重置随机时间
func (rf *Raft) resetTimer() {
	rf.passedMs = 0
	rf.timeoutNeedMs = getRand(MINTIMEOUT, MAXTIMEOUT)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.identity == LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	// 候选人的 term
	Term		 	int
	// 候选人在数组 Raft.peers 的下标
	CandidateId		int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	// 告诉候选人去更新自己的 term
	Term			int
	// 是否得到投票
	VoteGranted 	bool
}

type ApplyEntriesArgs struct {
	// leader 的 term
	Term 		int
	LeaderId 	int
}

type ApplyEntriesReply struct {
	// 让 leader 更新自己的 term
	Term 	int
	Success bool
}

// example RequestVote RPC handler.
// 1. 情况一：候选人 term 比 currentTerm 小
// 2. 情况二：当前服务器没有投票或者已经投过票给候选人
// 3. 情况三：当前服务器给别的候选人投过票

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%s RequestVote(), args: %+v", rf.String(), args)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.resetTimer()

	// 当前服务器需要变成 follower
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	if rf.voteFor == UNVOTE || rf.voteFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
	rf.mu.Lock()
	DPrintf("%s sendRequestVote to server %d", rf.String(), server)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) ApplyEntries(args *ApplyEntriesArgs, reply *ApplyEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("%s ApplyEntries(), args: %+v", rf.String(), args)

	// 情况一：leader 的 term < currentTerm
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.resetTimer()
	// 检查更新当前服务器的 term
	if rf.currentTerm < args.Term {
		rf.toFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendApplyEntries(server int, args *ApplyEntriesArgs, reply *ApplyEntriesReply) bool {
	// DPrintf("%s sendApplyEntries to server %d", rf.String(), server)
	ok := rf.peers[server].Call("Raft.ApplyEntries", args, reply)
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

	// Your code here (3B).


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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 辅助函数（调用者加锁）：从 candidate/leader 变成 follower
func (rf *Raft) toFollower(term int) {
	DPrintf("toFollower() %s", rf.String())
	rf.currentTerm = term
	rf.identity = FOLLOWER
	rf.voteFor = UNVOTE
	rf.votes = 0
}

// 辅助函数（调用者加锁）：从 follower 变成 candidate
func (rf *Raft) toCandidate() {
	DPrintf("toCandidate()1 %s", rf.String())
	if rf.identity == LEADER {
		panic("a leader to a candidate!")
	}

	rf.currentTerm ++
	rf.identity = CANDIDATE
	rf.voteFor = rf.me
	rf.votes = 1
	rf.passedMs = 0
	rf.timeoutNeedMs = getRand(MINTIMEOUT, MAXTIMEOUT)

	DPrintf("toCandidate()2 %s", rf.String())
}

// 辅助函数（调用者加锁）：从 candidate 变成 leader
func (rf *Raft) toLeader() {
	DPrintf("toLeader()1 %s", rf.String())
	if rf.identity != CANDIDATE {
		panic("illegal transition to a leader.")
	}

	rf.identity = LEADER
	DPrintf("toLeader()2 %s", rf.String())
	// 开始发送心跳
	go rf.sendHeartBeat()
}

func (rf *Raft) sendHeartBeat() {
	for !rf.killed() {
		startTerm, isLeader := rf.GetState()
		if !isLeader {
			break
		}

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			currentTerm, cIsLeader := rf.GetState()
			if currentTerm != startTerm || !cIsLeader {
				return
			}
			
			// 调用 ApplyEntries
			args := ApplyEntriesArgs{Term: currentTerm, LeaderId: rf.me}
			reply := ApplyEntriesReply{}

			ok := rf.sendApplyEntries(server, &args, &reply)
			if ok {
				currentTerm, cIsLeader := rf.GetState()

				if currentTerm != startTerm || !cIsLeader{
					return
				}
				if reply.Term > currentTerm {
					rf.mu.Lock()
					rf.toFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
			}
		}

		time.Sleep(time.Duration(HEARTBEATTIMEOUT) * time.Microsecond)
	}
}

// // 检查是否需要发起选举
// func (rf *Raft) needNewElection() bool {
// 	// 两种情况：
// 	// 1. follower 如果一段时间没有收到心跳且没有在给 candidate 投票，就发起选举
// 	// 2. candidate 发起选举一段时间后，没有成为 leader，重新发起选举

	
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// DPrintf("needNewElection() %s", rf.String())

// 	if rf.identity == CANDIDATE {
// 		return true
// 	}
// 	return rf.identity == FOLLOWER && 
// 	(!rf.receivedMsg)

// }

// 发起选举
func (rf *Raft) kickOffNewElection() {
	rf.mu.Lock()

		DPrintf("kickOffNewElection()-begin %s", rf.String())
		// 1. 成为候选人
		rf.toCandidate()
		startTerm := rf.currentTerm
		
	rf.mu.Unlock()
	
	// 2. 开启多个线程，同时向其他 server 拉票

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		
		go func(server int) {
			// 2.1 拉票之前检查时期有没有发生改变
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			if currentTerm != startTerm {
				return
			}
	
			// 2.2 调用 server 的 rpc
			args := RequestVoteArgs{Term: currentTerm, CandidateId: rf.me}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
	
			// 2.3 调用成功
			if ok {
				rf.mu.Lock()
				currentTerm = rf.currentTerm
				rf.mu.Unlock()
				// 2.3.1 等待投票后时期发生改变
				if currentTerm != startTerm {
					return
				}
	
				// 2.3.2 选举失败
				if reply.Term > currentTerm {
					rf.mu.Lock()
					rf.toFollower(reply.Term)
					rf.mu.Unlock()
					return
				} 
				// 2.3.3 得到投票
				if reply.VoteGranted {
					rf.mu.Lock()
					rf.votes ++
					rf.mu.Unlock()
				}
			}
		}(i)
	}
	
	// 3. 检查是否赢得选举
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == startTerm && rf.votes > len(rf.peers) - rf.votes {
		// 3.1 成为 leader
		rf.toLeader()
	}
	DPrintf("kickOffNewElection()-end %s, votes are %d", rf.String(), rf.votes)
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		// 检查是否需要重新选举(计时器是否结束)
		rf.mu.Lock()
		if rf.passedMs < rf.timeoutNeedMs {
			rf.passedMs ++
			rf.mu.Unlock()

			time.Sleep(1 * time.Millisecond)
			continue
		}
		rf.mu.Unlock()

		// 计时器结束
		// 两种情况：
		// 1. follower 如果一段时间没有收到心跳且没有在给 candidate 投票，就发起选举
		// 2. candidate 发起选举一段时间后，没有成为 leader，重新发起选举
		// 非 leader 就发起选举
		_, isLeader := rf.GetState()
		if !isLeader {
			rf.kickOffNewElection()
		}


		// // pause for a random amount of time between 50 and 350
		// // milliseconds.

		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// 3A
	rf.currentTerm = 0
	rf.identity = FOLLOWER
	rf.voteFor = UNVOTE
	rf.votes = 0
	rf.passedMs = 0
	rf.timeoutNeedMs = getRand(MINTIMEOUT, MAXTIMEOUT)
	DPrintf("Make() %s", rf.String())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
