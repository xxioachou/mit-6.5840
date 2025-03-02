package kvraft

import (
	"log"
	"strconv"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

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
	OpId			string			// 命令的唯一 id，避免重复执行某条命令
	GetValid		bool			// Get() 操作
	PutValid		bool			// Put() 操作
	AppendValid		bool			// Append() 操作
	Key				string	
	Value			string	
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data			map[string]string		// 存储的数据（k-v对）
	results			map[string]string		// 保存已经操作的结果(OpId-value)
	applyCond		*sync.Cond				// 执行某次操作之后唤醒 RPC 协程
}

// 检查 RPC handler 是否应该继续等待
// 任期没有改变并且操作没有完成
func (kv *KVServer) Check(opId string, startTerm int) bool {
	currentTerm, _ := kv.rf.GetState()
	_, ok := kv.results[opId]
	return currentTerm == startTerm && !ok
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[server %d] Get(args %+v)", kv.me, args)
	defer DPrintf("[server %d]Get(args %+v), reply %+v", kv.me, args, reply)

	// Your code here.
	opId := getKey(args.ClientID, args.CallID)
	kv.mu.Lock()
	v, ok := kv.results[opId]
	kv.mu.Unlock()
	if ok {
		reply.Err = OK
		reply.Value = v
		return
	}

	op := Op{
		OpId: opId,
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
	for kv.Check(opId, startTerm) {
		kv.applyCond.Wait()
	}
	defer kv.mu.Unlock()
	
	currentTerm, _ := kv.rf.GetState()
	if currentTerm != startTerm {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		reply.Value = kv.results[opId]
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[server %d] Put(args %+v)", kv.me, args)
	defer DPrintf("[server %d]Put(args %+v), reply %+v", kv.me, args, reply)
	// Your code here.

	opId := getKey(args.ClientID, args.CallID)
	kv.mu.Lock()
	_, ok := kv.results[opId]
	kv.mu.Unlock()
	if ok {
		reply.Err = OK
		return
	}

	op := Op{
		OpId: opId,
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
	for kv.Check(opId, startTerm) {
		kv.applyCond.Wait()
	}
	defer kv.mu.Unlock()
	
	currentTerm, _ := kv.rf.GetState()
	if currentTerm != startTerm {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[server %d] Append(args %+v)", kv.me, args)
	defer DPrintf("[server %d]Append(args %+v), reply %+v", kv.me, args, reply)
	// Your code here.

	opId := getKey(args.ClientID, args.CallID)
	kv.mu.Lock()
	_, ok := kv.results[opId]
	kv.mu.Unlock()
	if ok {
		reply.Err = OK
		return
	}

	op := Op{
		OpId: opId,
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
	for kv.Check(opId, startTerm) {
		DPrintf("[server %d] waitting...", kv.me)
		kv.applyCond.Wait()
	}
	defer kv.mu.Unlock()
	
	currentTerm, _ := kv.rf.GetState()
	if currentTerm != startTerm {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
}

// 从 applyCh 中读取已经提交的命令
func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if m.CommandValid {
			kv.mu.Lock()
			op := m.Command.(Op)
			kv.mu.Unlock()

			kv.execOp(op)
		}
	}
}

// 执行操作，将结果存放到 results, 唤醒对应的 RPC handler
func (kv *KVServer) execOp(op Op) {
	DPrintf("[server %d] execOp(op %+v)", kv.me, op)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 这条命令已经执行过
	_, ok := kv.results[op.OpId]
	if ok {
		return
	}

	if op.GetValid {
		v, ok := kv.data[op.Key]
		if ok {
			kv.results[op.OpId] = v
		} else {
			kv.results[op.OpId] = ""
		}
	} else if op.PutValid {
		kv.data[op.Key] = op.Value
		kv.results[op.OpId] = OK
	} else if op.AppendValid {
		value, ok := kv.data[op.Key]
		if !ok {
			value = ""
		}
		kv.data[op.Key] = value + op.Value
		kv.results[op.OpId] = OK
	}

	kv.applyCond.Broadcast()
}

func getKey(clientID, callID int64) string {
	return strconv.FormatInt(clientID, 10) + "," + strconv.FormatInt(callID, 10)
}

func (kv *KVServer) Succeed(args *SucceedArgs, reply *SucceedReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	opId := getKey(args.ClientID, args.CallID)
	delete(kv.results, opId)
	*reply = true
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
	kv.data = make(map[string]string)
	kv.results = make(map[string]string)
	kv.applyCond = sync.NewCond(&kv.mu)
	go kv.applier()

	return kv
}
