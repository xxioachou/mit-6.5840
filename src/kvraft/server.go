package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false
const SnapshotTimeout = 100

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId		int64			
	CallId			int64
	GetValid		bool			// Get() 操作
	PutValid		bool			// Put() 操作
	AppendValid		bool			// Append() 操作
	Key				string	
	Value			string	
}

type LastOperation struct {
	CallId 			int64
	Result 			string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Data				map[string]string		 // 存储的数据（k-v对）
	LastOp				map[int64]*LastOperation // clientId-last operation
	applyCond			*sync.Cond				 // 执行某次操作之后唤醒 RPC 协程

	LastApplied			int						 // 状态机最后应用的日志索引	
	persister 			*raft.Persister
}

// 检查 RPC handler 是否应该继续等待
// 任期没有改变并且操作没有完成
func (kv *KVServer) Check(clientId, callId int64, startTerm int) bool {
	currentTerm, _ := kv.rf.GetState()
	v, ok := kv.LastOp[clientId]
	return currentTerm == startTerm && (!ok || v.CallId < callId)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[server %d] Get(args %+v)", kv.me, args)
	defer DPrintf("[server %d] Get(args %+v), reply %+v", kv.me, args, reply)

	// Your code here.
	kv.mu.Lock()
	v, ok := kv.LastOp[args.ClientID]
	kv.mu.Unlock()

	// 这是一条旧的 RPC 请求
	if ok && args.CallID <= v.CallId {
		if args.CallID == v.CallId {
			reply.Err = OK
			reply.Value = v.Result
		} else {
			reply.Err = ErrWrongLeader
		}
		return
	}

	op := Op{
		ClientId: args.ClientID,
		CallId: args.CallID,
		GetValid: true,
		Key: args.Key,
	}
	_, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 当前服务器不是 leader
		reply.Err = ErrWrongLeader
		return
	}


	kv.mu.Lock()
	for kv.Check(args.ClientID, args.CallID, startTerm) {
		kv.applyCond.Wait()
	}
	defer kv.mu.Unlock()
	
	currentTerm, _ := kv.rf.GetState()
	if currentTerm != startTerm {
		reply.Err = ErrWrongLeader
	} else {
		v, ok := kv.LastOp[args.ClientID]
		if !ok || v.CallId != args.CallID {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			reply.Value = v.Result
		}
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[server %d] Put(args %+v)", kv.me, args)
	defer DPrintf("[server %d] Put(args %+v), reply %+v", kv.me, args, reply)
	// Your code here.

	kv.mu.Lock()
	v, ok := kv.LastOp[args.ClientID]
	kv.mu.Unlock()

	// 这是一条旧的 RPC 请求
	if ok && args.CallID <= v.CallId {
		if args.CallID == v.CallId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
		return
	}

	op := Op{
		ClientId: args.ClientID,
		CallId: args.CallID,
		PutValid: true,
		Key: args.Key,
		Value: args.Value,
	}
	_, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 当前服务器不是 leader
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	for kv.Check(args.ClientID, args.CallID, startTerm) {
		kv.applyCond.Wait()
	}
	defer kv.mu.Unlock()
	
	currentTerm, _ := kv.rf.GetState()
	if currentTerm != startTerm {
		reply.Err = ErrWrongLeader
	} else {
		v, ok := kv.LastOp[args.ClientID]
		if !ok || v.CallId != args.CallID {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[server %d] Append(args %+v)", kv.me, args)
	defer DPrintf("[server %d] Append(args %+v), reply %+v", kv.me, args, reply)
	// Your code here.

	kv.mu.Lock()
	v, ok := kv.LastOp[args.ClientID]
	kv.mu.Unlock()

	// 这是一条旧的 RPC 请求
	if ok && args.CallID <= v.CallId {
		if args.CallID == v.CallId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
		return
	}

	op := Op{
		ClientId: args.ClientID,
		CallId: args.CallID,
		AppendValid: true,
		Key: args.Key,
		Value: args.Value,
	}
	_, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 当前服务器不是 leader
		reply.Err = ErrWrongLeader
		return
	}
		
	// 等待
	kv.mu.Lock()
	for kv.Check(args.ClientID, args.CallID, startTerm) {
		DPrintf("[server %d] waitting...", kv.me)
		kv.applyCond.Wait()
	}
	defer kv.mu.Unlock()
	
	currentTerm, _ := kv.rf.GetState()
	if currentTerm != startTerm {
		reply.Err = ErrWrongLeader
	} else {
		v, ok := kv.LastOp[args.ClientID]
		if !ok || v.CallId != args.CallID {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	}
}

// 从 applyCh 中读取已经提交的命令
func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if m.CommandValid {		
			// 提交了命令
			op := m.Command.(Op)
			kv.execOp(op, m.CommandIndex)

		} else if m.SnapshotValid {
			// 提交了日志
			kv.applySnapshot(m.SnapshotIndex, m.Snapshot)
		}
	}
}

// 执行操作，将结果存放到 results, 唤醒对应的 RPC handler
func (kv *KVServer) execOp(op Op, index int) {
	DPrintf("[server %d] execOp(op %+v, index %v)", kv.me, op, index)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.LastApplied >= index {
		panic(fmt.Sprintf("kv.lastApplied %v >= m.CommandIndex %v", kv.LastApplied, index))
	}
	if kv.LastApplied + 1 != index {
		panic(fmt.Sprintf("kv.lastApplied %v + 1 != m.CommandIndex %v", kv.LastApplied, index))
	}
	kv.LastApplied ++

	v, ok := kv.LastOp[op.ClientId]
	// 这条命令已经执行过
	if ok && v.CallId >= op.CallId {
		return
	}
	
	lop := LastOperation{CallId: op.CallId}
	if op.GetValid {
		v, ok := kv.Data[op.Key]
		if ok {
			lop.Result = v
		} else {
			lop.Result = ""
		}
	} else if op.PutValid {
		kv.Data[op.Key] = op.Value
	} else if op.AppendValid {
		value, ok := kv.Data[op.Key]
		if !ok {
			value = ""
		}
		kv.Data[op.Key] = value + op.Value
	}
	kv.LastOp[op.ClientId] = &lop

	kv.applyCond.Broadcast()
}

func (kv *KVServer) makeSnapshot() {
	for !kv.killed() {
		if kv.persister.RaftStateSize() >= kv.maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			kv.mu.Lock()
			LastApplied := kv.LastApplied
			e.Encode(LastApplied)
			e.Encode(kv.Data)
			e.Encode(kv.LastOp)
			kv.mu.Unlock()

			DPrintf("[server %d] makeSnapshot(): LastApplied %v", kv.me, LastApplied)
			kv.rf.Snapshot(LastApplied, w.Bytes())
		}
		time.Sleep(SnapshotTimeout * time.Millisecond)
	}
}

func (kv *KVServer) applySnapshot(snapshotIndex int, snapshot []byte) {
	DPrintf("[server %d] applySnapshot(snapshotIndex %v)", kv.me, snapshotIndex)

	w := bytes.NewBuffer(snapshot)
	e := labgob.NewDecoder(w)
	var index int
	data := make(map[string]string)
	lastOp := make(map[int64]*LastOperation)

	if err := e.Decode(&index); err != nil {
		log.Printf("applySnapshot()" + err.Error())
	}
	if err := e.Decode(&data); err != nil {
		log.Printf("applySnapshot()" + err.Error())
	}
	if err := e.Decode(&lastOp); err != nil {
		log.Printf("applySnapshot()" + err.Error())
	}

	if index != snapshotIndex {
		panic(fmt.Sprintf("applySnapshot():index %v != snapshotIndex %v", index, snapshotIndex))
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 过期的快照
	if snapshotIndex < kv.LastApplied {
		return
	}

	kv.LastApplied = snapshotIndex
	kv.Data = data
	kv.LastOp = lastOp
}

func (kv *KVServer) restoreState(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var lastApplied int
	data := make(map[string]string)
	lastOp := make(map[int64]*LastOperation)

	w := bytes.NewBuffer(snapshot)
	e := labgob.NewDecoder(w)
	if e.Decode(&lastApplied) != nil ||
		e.Decode(&data) != nil || 
		e.Decode(&lastOp) != nil {
		log.Printf("[server %d] restoreState error!", kv.me)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.LastApplied = lastApplied
	kv.Data = data
	kv.LastOp = lastOp
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("[server %d] Kill()", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	DPrintf("[server %d] StartKVServer()", me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.Data = make(map[string]string)
	kv.LastOp = make(map[int64]*LastOperation)
	kv.applyCond = sync.NewCond(&kv.mu)

	kv.LastApplied = 0
	kv.persister = persister

	// 恢复到保存的快照状态
	kv.restoreState(kv.persister.ReadSnapshot())


	go kv.applier()

	if maxraftstate != -1 {
		go kv.makeSnapshot()
	}
	return kv
}
