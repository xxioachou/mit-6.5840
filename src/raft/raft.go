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

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	Command 	interface{}			// 命令
	Term		int					// Leader 收到命令时的 term
}

const (
	FOLLOWER	= iota
	CANDIDATE
	LEADER
)

const Unvote = -1
const InvalidIndex = -1
const MinTimeout = 300
const MaxTimeout = 600
const HeartbeatTimeout = 100
const CheckTimeout = 50
const ChanSize = 30

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

	CurrentTerm			int				// 当前服务器的 term
	identity 			int				// 当前服务器的身份 (follower,candidate,leader)
	VoteFor				int				// 当前服务器给哪个候选人投票
	changeChan			chan int		// 状态改变时用于通知 ticker

	applyChan			chan ApplyMsg	
	Logs				[]LogEntry		// 当前服务器维护的 logs(下标从 1 开始)
	committedIndex		int				// 已知已提交的最高的日志条目的索引
	lastApplied			int				// 已经被应用到状态机的最高日志条目的索引

	nextIndex			[]int			// 每台服务器应当被发送的下一个条目的索引
	matchIndex			[]int			// 每台服务器已知的已经复制到该服务器的最高的日志条目的索引
}

// 返回 [l, r] 之间的随机数
func getRand(l, r int) int {
	len := int64(r - l + 1)
	return l + int(rand.Int63() % len)
}

func (rf *Raft) String() string {
	var str strings.Builder
	str.Write([]byte(fmt.Sprintf("server: %d", rf.me)))
	str.Write([]byte(fmt.Sprintf(", currentTerm: %d", rf.CurrentTerm)))
	var identity string
	if rf.identity == LEADER {
		identity = "leader"
	} else if rf.identity == CANDIDATE {
		identity = "candidate"
	} else if rf.identity == FOLLOWER {
		identity = "follower"
	}
	str.Write([]byte(fmt.Sprintf(", identity: %s", identity)))
	str.Write([]byte(fmt.Sprintf(", voteFor: %d\n", rf.VoteFor)))
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

	term = rf.CurrentTerm
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
// 调用之前需要加锁
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Logs)
	rf.persister.Save(w.Bytes(), nil)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	
	var currentTerm int
	var voteFor int
	var logs []LogEntry

	if err := d.Decode(&currentTerm); err != nil {
		log.Printf("[server %d] readPersist: " + err.Error(), rf.me)
		return
	}
	if err := d.Decode(&voteFor); err != nil {
		log.Printf("[server %d] readPersist: " + err.Error(), rf.me)
		return
	}
	if err := d.Decode(&logs); err != nil {
		log.Printf("[server %d] readPersist: " + err.Error(), rf.me)
		return
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.CurrentTerm = currentTerm
	rf.VoteFor = voteFor
	rf.Logs = logs
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
	Term		 	int				// 候选人的 term
	CandidateId		int				// 候选人 id
	
	LastLogIndex	int				// 候选人最后日志条目的索引值
	LastLogTerm		int				// 候选人最后日志条目的 term
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
	Term 			int				// leader 的 term
	LeaderId 		int				// leader 的 id

	PrevLogIndex	int				// 紧邻新日志条目的前一个条目的索引
	PrevLogTerm		int				// 紧邻新日志条目的前一个条目的任期
	Entries			[]LogEntry		// 需要被保存的日志条目
	LeaderCommit	int				// 领导人已知已提交的最高的日志条目索引
}

type ApplyEntriesReply struct {
	// 让 leader 更新自己的 term
	Term 	int
	Success bool

	XTerm	int		// 冲突日志条目的任期
	XIndex 	int		// XTerm 对应的第一个日志条目的索引
	XLen	int		// 日志长度
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

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	// 当前服务器需要变成 follower
	if args.Term > rf.CurrentTerm {
		rf.changeChan <- 1 
		rf.CurrentTerm = args.Term
		rf.identity = FOLLOWER
		rf.VoteFor = Unvote

		// 3C
		rf.persist()
	}

	lastLogIndex := len(rf.Logs) - 1
	lastLogTerm := rf.Logs[lastLogIndex].Term
	// 候选人的日志至少和当前服务器的日志一样新(3B)
	ok := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if !ok {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if (rf.VoteFor == Unvote || rf.VoteFor == args.CandidateId) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.CurrentTerm = args.Term
		rf.identity = FOLLOWER
		rf.VoteFor = args.CandidateId

		// 3C
		rf.persist()
	} else {
		reply.Term = rf.CurrentTerm
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
	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	
	rf.changeChan <- 1 
	rf.identity = FOLLOWER
	// 检查更新当前服务器的 term
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VoteFor = Unvote

		// 3C
		rf.persist()
	}
	
	reply.Term = rf.CurrentTerm
	// follower 找不到一样的日志条目（一致性检查）
	if (args.PrevLogIndex >= len(rf.Logs) || rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false

		if args.PrevLogIndex >= len(rf.Logs) {
			reply.XIndex = InvalidIndex
			reply.XLen = len(rf.Logs)
		} else {
			reply.XTerm = rf.Logs[args.PrevLogIndex].Term
			index := rf.findFirstIndex(reply.XTerm)
			if index >= len(rf.Logs) || rf.Logs[index].Term != reply.XTerm {
				panic("rf.findFirstIndex: error!")
			}
			reply.XIndex = index
			reply.XLen = len(rf.Logs)
		}
	} else {
		reply.Success = true
		// 复制日志 RPC
		if len(args.Entries) > 0 {
			// 当前复制条目的索引
			index := args.PrevLogIndex + 1
			log := args.Entries[0]
			if index >= len(rf.Logs) {
				// 追加条目
				rf.Logs = append(rf.Logs, log)
			} else {
				if rf.Logs[index].Term != log.Term {
					// 发生冲突时需要把 >= index 的条目都删除
					for len(rf.Logs) - 1 >= index {
						rf.Logs = rf.Logs[:len(rf.Logs) - 1]
					}
					rf.Logs = append(rf.Logs, log)
				}
			}
			if args.LeaderCommit > rf.committedIndex {
				if args.LeaderCommit > index {
					rf.committedIndex = index
				} else {
					rf.committedIndex = args.LeaderCommit
				}
			}

			// 3C
			rf.persist()
		} else {
			// 心跳 RPC
			if args.LeaderCommit > rf.committedIndex {
				if args.LeaderCommit >= len(rf.Logs) - 1 {
					rf.committedIndex = len(rf.Logs) - 1
				} else {
					rf.committedIndex = args.LeaderCommit
				}
			}
		}

	}

}

func (rf *Raft) sendApplyEntries(server int, args *ApplyEntriesArgs, reply *ApplyEntriesReply) bool {
	// DPrintf("%s sendApplyEntries to server %d", rf.String(), server)
	ok := rf.peers[server].Call("Raft.ApplyEntries", args, reply)
	return ok
}

// 返回第一个任期 >= term 的日志的索引（调用者需要加锁）
func (rf *Raft) findFirstIndex(term int) int {
	low, high := 1, len(rf.Logs) - 1
	for low < high {
		mid := (low + high) / 2
		if rf.Logs[mid].Term >= term {
			high = mid
		} else {
			low = mid + 1
		}
	}
	if rf.Logs[high].Term < term {
		return len(rf.Logs)
	}
	return high
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
	isLeader := false
	// isLeader := false

	// Your code here (3B).	
	if !rf.killed(){
		rf.mu.Lock()
		if rf.identity == LEADER {
			index = len(rf.Logs)
			rf.Logs = append(rf.Logs, LogEntry{Command: command, Term: rf.CurrentTerm})
			term = rf.CurrentTerm
			isLeader = true

			DPrintf("rf.Start(): command %+v, leader %d, leader's logs %+v\n", command, rf.me, rf.Logs)

			// 3C
			rf.persist()
		}
		rf.mu.Unlock()
	}

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
	args := ApplyEntriesArgs{
		Term: rf.CurrentTerm,
		LeaderId: rf.me,
		LeaderCommit: rf.committedIndex,
	}
	logs := make([]LogEntry, len(rf.Logs))
	nextIndex := make([]int, len(rf.nextIndex))
	copy(logs, rf.Logs)
	copy(nextIndex, rf.nextIndex)
	// DPrintf("[Leader %d] sendHeartBeat, currentTerm is %d, logs are %v, nextIndex are %v, matchIndex are %v", rf.me, currentTerm, rf.Logs, rf.nextIndex, rf.matchIndex)
	rf.mu.Unlock()

	// 同时向其他服务器发送心跳
	// 如果有日志需要发送，就添加到 args 上(3B)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// 调用 ApplyEntries

		lastLogIndex := len(logs) - 1
		if lastLogIndex >= nextIndex[i] {
			index := nextIndex[i]
			if index == 0 {
				panic("nextIndex[i] == 0")
			}

			args.PrevLogIndex = index - 1
			args.PrevLogTerm = logs[index - 1].Term
			args.Entries = []LogEntry{logs[index]}
		} else {
			args.PrevLogIndex = lastLogIndex
			args.PrevLogTerm = logs[lastLogIndex].Term
			args.Entries = []LogEntry{}
		}

		go func(server int, args ApplyEntriesArgs) {
			DPrintf("[Leader %d] send heartbeat to server %d, args %+v", rf.me, server, args)
			reply := ApplyEntriesReply{}
			ok := rf.sendApplyEntries(server, &args, &reply)
			if !ok {
				return
			}
			
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term < rf.CurrentTerm {	
				// 过时的 RPC
				return
			}

			if !reply.Success {
				if reply.Term > rf.CurrentTerm {
					// 因 leader 的任期过时被拒绝，变成 follower
					rf.changeChan <- 1
					rf.CurrentTerm = reply.Term
					rf.identity = FOLLOWER
					rf.VoteFor = Unvote
	
					// 3C
					rf.persist()
				} else {
					if len(args.Entries) > 0 {
						// 因为没有通过一致性检查被拒绝，更新 nextIndex 重试

						if reply.XIndex == InvalidIndex {
							// server 日志太短
							rf.nextIndex[server] = reply.XLen
						} else {
							index := rf.findFirstIndex(reply.XTerm)
							if index == len(rf.Logs) || rf.Logs[index].Term != reply.XTerm {
								// leader 不存在 reply.XTerm 的记录
								rf.nextIndex[server] = reply.XIndex
							} else {
								index2 := rf.findFirstIndex(reply.XTerm + 1)
								if rf.Logs[index2 - 1].Term != reply.XTerm {
									str := fmt.Sprintf("rf.Logs[index2 - 1].Term != reply.XTerm, index2 is %d, log is %+v, reply.XTerm is %d", index2, rf.Logs[index2 - 1], reply.XTerm)
									panic(str)
								}
								rf.nextIndex[server] = index2 - 1
							}
						}
					}
				}
			} else {
				// 成功调用，更新 nextIndex
				if len(args.Entries) > 0 {
					index := args.PrevLogIndex + 1
					if index == rf.nextIndex[server] {
						rf.matchIndex[server] = index
						rf.nextIndex[server]  = index + 1
						DPrintf("[Leader %d] succeeded to copyEntries to server %d, args %+v", rf.me, server, args)
					}
				}
			}

		}(i, args)
	}

}


// 发起选举
func (rf *Raft) kickOffNewElection() {
	var votes int32
	atomic.StoreInt32(&votes, 1)

	rf.mu.Lock()
	args := RequestVoteArgs{
		Term: rf.CurrentTerm, 
		CandidateId: rf.me,
		LastLogIndex: len(rf.Logs) - 1,
		LastLogTerm: rf.Logs[len(rf.Logs) - 1].Term,
	}
	rf.mu.Unlock()
	DPrintf("[server %d] kickOffNewElection, args is %+v", rf.me, args)

	// 1. 开启多个线程，同时向其他 server 拉票
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		
		go func(server int, args RequestVoteArgs) {	
			reply := RequestVoteReply{}
			DPrintf("[server %d] sendRequestVote to %d", rf.me, server)
			ok := rf.sendRequestVote(server, &args, &reply)
	
			if !ok {
				DPrintf("[server %d] failed to sendRequestVote to %d, args %+v", rf.me, server, args)
			}
			if ok {
				DPrintf("[server %d] succeed to sendRequestVote to %d, args %+v, reply is %+v", rf.me, server, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term < rf.CurrentTerm {
					// 过时的 RPC
					return
				}

				if reply.Term > rf.CurrentTerm {
					rf.changeChan <- 1
					rf.CurrentTerm = reply.Term
					rf.identity = FOLLOWER
					rf.VoteFor = Unvote

					// 3C
					rf.persist()
				} else if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
					if int(atomic.LoadInt32(&votes)) * 2 > len(rf.peers) &&
						reply.Term == rf.CurrentTerm &&
						rf.identity == CANDIDATE {

						rf.changeChan <- 1
						rf.identity = LEADER

						// 初始化 nextIndex、matchIndex (3B)
						for j := range rf.peers {
							// 假设所有 Follower 的日志跟自己一致，后续再通过 rpc 调整
							rf.nextIndex[j] = len(rf.Logs)
						}
						for j := range rf.peers {
							rf.matchIndex[j] = 0
						}
					}
				}
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
				rf.mu.Lock()
				DPrintf("[server %d] changeChan: follower to %d\n", rf.me, rf.identity)
				rf.mu.Unlock()
			case <-time.After(time.Duration(getRand(MinTimeout, MaxTimeout)) * time.Millisecond):
				rf.mu.Lock()
				rf.identity = CANDIDATE
				DPrintf("[server %d] timeout...: follower to %d\n", rf.me, rf.identity)
				rf.mu.Unlock()
			}
		}
		case CANDIDATE: {
			rf.mu.Lock()
			rf.CurrentTerm ++
			rf.VoteFor = rf.me
			// 3C
			rf.persist()

			rf.mu.Unlock()
			go rf.kickOffNewElection()
			select {
			case <-rf.changeChan:
				rf.mu.Lock()
				DPrintf("[server %d] changeChan: candidate to %d\n", rf.me, rf.identity)
				rf.mu.Unlock()
			case <-time.After(time.Duration(getRand(MinTimeout, MaxTimeout)) * time.Millisecond):
				rf.mu.Lock()
				DPrintf("[server %d] timeout...: candidate to %d\n", rf.me, rf.identity)
				rf.mu.Unlock()
			}
		}
		case LEADER: {
			go rf.sendHeartBeat()
			select {
			case <-rf.changeChan:
				rf.mu.Lock()
				DPrintf("[server %d] changeChan: leader to %d\n", rf.me, rf.identity)
				rf.mu.Unlock()
			case <-time.After(time.Duration(HeartbeatTimeout) * time.Millisecond):
				rf.mu.Lock()
				DPrintf("[server %d] timeout...: leader to %d\n", rf.me, rf.identity)
				rf.mu.Unlock()
			}
		}
		}

		// // pause for a random amount of time between 50 and 350
		// // milliseconds.

		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 定时检查是否需要应用 log entry
func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		// DPrintf("[server %d] logs are %+v", rf.me, rf.logs)
		if rf.committedIndex > rf.lastApplied {
			DPrintf("[server %d] rf.committedIndex is %d, rf.lastApplied is %d", rf.me, rf.committedIndex, rf.lastApplied)
			rf.lastApplied ++
			msg := ApplyMsg{
				CommandValid: true,
				Command: rf.Logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyChan <- msg
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(CheckTimeout) * time.Millisecond)
		// time.Sleep(time.Duration(HEARTBEATTIMEOUT) * time.Millisecond)
	}
}

// Leader 不断检查是否需要更新 committedIndex
func (rf *Raft) updateCommittedIndex() {
	for !rf.killed() {
		_, isLeader := rf.GetState()
		if isLeader {
			rf.mu.Lock()
			i := rf.committedIndex + 1
			n := len(rf.Logs) - 1
			res := rf.committedIndex
			rf.mu.Unlock()

			for i <= n {
				rf.mu.Lock()
				if rf.Logs[i].Term != rf.CurrentTerm {
					rf.mu.Unlock()
					i ++
					continue
				}	
				rf.mu.Unlock()

				cnt := 1
				ok := false
				for j := range rf.peers {
					if j == rf.me {
						continue
					}

					rf.mu.Lock()
					matchIndex := rf.matchIndex[j]
					rf.mu.Unlock()
					if matchIndex >= i {
						cnt ++
						if cnt > len(rf.peers) - cnt {
							ok = true
							break
						}
					}
				}
				if ok {
					res = i
				}
				i ++
			}

			rf.mu.Lock()
			if rf.committedIndex != res && rf.identity == LEADER {
				DPrintf("[Leader %d] change committedIndex %d to %d", rf.me, rf.committedIndex, res)
				rf.committedIndex = res
			}
			rf.mu.Unlock()
		}

		time.Sleep(time.Millisecond * time.Duration(CheckTimeout))
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
	rf.CurrentTerm = 0
	rf.identity = FOLLOWER
	rf.VoteFor = Unvote
	rf.changeChan = make(chan int, ChanSize)
	// 3B
	rf.applyChan = applyCh
	rf.Logs = make([]LogEntry, 1)			// 下标从 1 开始
	rf.committedIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// DPrintf("Make() %s", rf.String())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections(3A)
	go rf.ticker()
	// 3B
	go rf.applyLog()
	go rf.updateCommittedIndex()

	return rf
}
