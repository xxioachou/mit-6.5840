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
const MINTIMEOUT = 300
const MAXTIMEOUT = 500
const HEARTBEATTIMEOUT = 100
const CHANSIZE = 30

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

	currentTerm			int			// 当前服务器的 term
	identity 			int			// 当前服务器的身份 (follower,candidate,leader)
	voteFor				int			// 当前服务器给哪个候选人投票
	changeChan			chan int	// 清空计时器时用于通知 ticker

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
	str.Write([]byte(fmt.Sprintf(", voteFor: %d\n", rf.voteFor)))
	return str.String()
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
	// DPrintf("%s RequestVote(), args: %+v", rf.String(), args)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 当前服务器需要变成 follower
	if args.Term > rf.currentTerm {
		rf.changeChan <- 1 
		rf.currentTerm = args.Term
		rf.identity = FOLLOWER
		rf.voteFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	if rf.voteFor == UNVOTE || rf.voteFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
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
	// rf.mu.Lock()
	// DPrintf("%s sendRequestVote to server %d", rf.String(), server)
	// rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) ApplyEntries(args *ApplyEntriesArgs, reply *ApplyEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 情况一：leader 的 term < currentTerm
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	
	rf.changeChan <- 1 
	rf.identity = FOLLOWER

	// 检查更新当前服务器的 term
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.voteFor = UNVOTE
	}
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

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	// 调用 ApplyEntries
	args := ApplyEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
	rf.mu.Unlock()

	// 同时向其他服务器发送心跳
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, args ApplyEntriesArgs) {
			reply := ApplyEntriesReply{}
			ok := rf.sendApplyEntries(server, &args, &reply)
			if ok {
				rf.mu.Lock()	
				if reply.Term > rf.currentTerm {
					rf.changeChan <- 1
					rf.currentTerm = reply.Term
					rf.identity = FOLLOWER
					rf.voteFor = UNVOTE
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}(i, args)
	}

}


// 发起选举
func (rf *Raft) kickOffNewElection() {
	var votes int32
	atomic.StoreInt32(&votes, 1)

	rf.mu.Lock()
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	rf.mu.Unlock()

	// 1. 开启多个线程，同时向其他 server 拉票
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		
		go func(server int, args RequestVoteArgs) {	
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
	
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.changeChan <- 1
					rf.currentTerm = reply.Term
					rf.identity = FOLLOWER
					rf.voteFor = UNVOTE
				} else if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
					if int(atomic.LoadInt32(&votes)) * 2 > len(rf.peers) &&
						reply.Term == rf.currentTerm &&
						rf.identity == CANDIDATE {
						rf.changeChan <- 1
						rf.identity = LEADER
					}
				}
				rf.mu.Unlock()
			}
		}(i, args)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		identity := rf.identity
		rf.mu.Unlock()

		switch identity {
		case FOLLOWER: {
			select {
			case <-rf.changeChan:
				// rf.mu.Lock()
				// DPrintf("server %d,changeChan: follower to %d\n", rf.me, rf.identity)
				// rf.mu.Unlock()
			case <-time.After(time.Duration(getRand(MINTIMEOUT, MAXTIMEOUT)) * time.Millisecond):
				rf.mu.Lock()
				rf.identity = CANDIDATE
				DPrintf("server %d,timeout...: follower to %d\n", rf.me, rf.identity)
				rf.mu.Unlock()
			}
		}
		case CANDIDATE: {
			rf.mu.Lock()
			rf.currentTerm ++
			rf.voteFor = rf.me
			// DPrintf("server %d,kickOffNewElection...", rf.me)
			rf.mu.Unlock()
			go rf.kickOffNewElection()
			select {
			case <-rf.changeChan:
				// rf.mu.Lock()
				// DPrintf("server %d,changeChan: candidate to %d\n", rf.me, rf.identity)
				// rf.mu.Unlock()
			case <-time.After(time.Duration(getRand(MINTIMEOUT, MAXTIMEOUT)) * time.Millisecond):
				// rf.mu.Lock()
				// DPrintf("server %d,timeout...: candidate to %d\n", rf.me, rf.identity)
				// rf.mu.Unlock()
			}
		}
		case LEADER: {
			// DPrintf("server %d,sendHeartBeat...", rf.me)
			go rf.sendHeartBeat()
			select {
			case <-rf.changeChan:
				// rf.mu.Lock()
				// DPrintf("server %d,changeChan: leader to %d\n", rf.me, rf.identity)
				// rf.mu.Unlock()
			case <-time.After(time.Duration(HEARTBEATTIMEOUT) * time.Millisecond):
				// rf.mu.Lock()
				// DPrintf("server %d,timeout...: leader to %d\n", rf.me, rf.identity)
				// rf.mu.Unlock()
			}
		}
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
	rf.changeChan = make(chan int, CHANSIZE)
	// DPrintf("Make() %s", rf.String())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
