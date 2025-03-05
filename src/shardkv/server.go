package shardkv

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
	"6.5840/shardctrler"
)

type OpType int

const (
	Get = iota
	Put
	Append
	Reconfigure
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   OpType
	ClientId int64
	CallId   int64
	Key      string
	Value    string

	// Reconfigure 的参数
	Servers	 []string				// 发送给哪个组
	Data	 map[string]string		// 要发送的数据
}

type LastOperation struct {
	CallId int64
	Result string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead				int32
	Data				map[string]string		 // 存储的数据（k-v对）
	sc          		*shardctrler.Clerk       // 分片控制器
	notifyCond  		*sync.Cond               // 用于通知 RPC 协程操作完成
	LastOp      		map[int64]*LastOperation // clientId -> *last operation，用来处理重复的 RPC 操作
	LastApplied 		int                      // 最近收到的已经提交的日志索引
	persister 			*raft.Persister
	config				shardctrler.Config		 // 最近收到的 config
	isReceivingFrom		[shardctrler.NShards]int // i -> gid，正在从 gid 接收 shard i 的数据(k-v对)
}

// 检查某次 RPC 调用对应的操作是否已经执行
func (kv *ShardKV) checkExecuted(clientId, callId int64, reply interface{}, opType OpType) bool {
	kv.mu.Lock()
	v, ok := kv.LastOp[clientId]
	kv.mu.Unlock()
	if !ok || v.CallId < callId {
		// 没有执行	
		return false
	}


	eq := v.CallId == callId

	switch opType {
	case Get:
		if eq {
			reply.(*GetReply).Err = OK
			reply.(*GetReply).Value = v.Result
		} else {
			reply.(*GetReply).Err = ErrWrongLeader
		}
	case Put, Append:
		if eq {
			reply.(*PutAppendReply).Err = OK
		} else {
			reply.(*PutAppendReply).Err = ErrWrongLeader
		}
	default:
		DPrintf("[gid %d][server %d] checkExecuted(opType %v)", kv.gid, kv.me, opType)
	}

	return true
}

// 检查是否应该当前组来处理这个 RPC 请求（调用者加锁）
func (kv *ShardKV) checkRightGroup(key string) bool {
	shard := key2shard(key)
	gid := kv.config.Shards[shard]
	return gid == kv.gid
}

// 正在接收 key 对应 shard 的数据（调用者加锁）
func (kv *ShardKV) checkReceivingData(key string) bool {
	shard := key2shard(key)
	return kv.isReceivingFrom[shard] != InvalidGid
}

// 检查是否需要继续等待(任期改变或者 index 对应的操作被提交或者组改变，返回 false)（调用者加锁）
func (kv *ShardKV) check(startTerm int, index int, key string) bool {
	currentTerm, _ := kv.rf.GetState()
	return currentTerm == startTerm && kv.LastApplied < index && kv.checkRightGroup(key)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[gid %d][server %d] Get(args %+v)", kv.gid, kv.me, args)
	defer func() { DPrintf("[gid %d][server %d] Get(args %+v), reply %+v", kv.gid, kv.me, args, reply) }()
	// Your code here.
	// 检查是否已经执行，如果已经执行填入 reply
	if kv.checkExecuted(args.Identifier.ClientId, args.Identifier.CallId, reply, Get) {
		return
	}

	kv.mu.Lock()
	rightGroup := kv.checkRightGroup(args.Key)
	kv.mu.Unlock()
	if !rightGroup {
		reply.Err = ErrWrongGroup
		return
	}

	kv.mu.Lock()
	receiving := kv.checkReceivingData(args.Key)
	kv.mu.Unlock()
	if receiving {
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{
		OpType: Get,
		ClientId: args.Identifier.ClientId,
		CallId: args.Identifier.CallId,
		Key: args.Key,
	}
	index, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}


	kv.mu.Lock()
	for kv.check(startTerm, index, args.Key) {
		kv.notifyCond.Wait()
	}
	defer kv.mu.Unlock()

	currentTerm, _ := kv.rf.GetState()
	if !kv.checkRightGroup(args.Key) {
		reply.Err = ErrWrongGroup
	} else if currentTerm != startTerm {
		reply.Err = ErrWrongLeader
	} else {
		v, ok := kv.LastOp[args.Identifier.ClientId]
		if !ok || v.CallId != args.Identifier.CallId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			reply.Value = v.Result
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[gid %d][server %d] PutAppend(args %+v)", kv.gid, kv.me, args)
	defer func() { DPrintf("[gid %d][server %d] PutAppend(args %+v), reply %+v", kv.gid, kv.me, args, reply) }()
	// Your code here.
	var opType OpType
	if args.Op == "Put" {
		opType = Put
	} else if args.Op == "Append" {
		opType = Append
	} else {
		DPrintf("[gid %d][server %d] Unknow op %v", kv.gid, kv.me, args.Op)
		return
	}

	// 检查是否已经执行，如果已经执行填入 reply
	if kv.checkExecuted(args.Identifier.ClientId, args.Identifier.CallId, reply, opType) {
		return
	}

	kv.mu.Lock()
	rightGroup := kv.checkRightGroup(args.Key)
	kv.mu.Unlock()
	if !rightGroup {
		reply.Err = ErrWrongGroup
		return
	}

	kv.mu.Lock()
	receiving := kv.checkReceivingData(args.Key)
	kv.mu.Unlock()
	if receiving {
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{
		OpType: opType,
		ClientId: args.Identifier.ClientId,
		CallId: args.Identifier.CallId,
		Key: args.Key,
		Value: args.Value,
	}
	index, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}


	kv.mu.Lock()
	// 阻塞等待，直到任期改变或者 index 对应的操作被提交
	for kv.check(startTerm, index, args.Key) {
		kv.notifyCond.Wait()
	}
	defer kv.mu.Unlock()


	currentTerm, _ := kv.rf.GetState()
	if !kv.checkRightGroup(args.Key) {
		reply.Err = ErrWrongGroup
	} else if currentTerm != startTerm {
		reply.Err = ErrWrongLeader
	} else {
		v, ok := kv.LastOp[args.Identifier.ClientId]
		if !ok || v.CallId != args.Identifier.CallId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	}
}

func (kv *ShardKV) ReceiveShardData(args *ReceiveShardDataArgs, reply *ReceiveShardDataReply) {
	DPrintf("[gid %d][server %d] ReceiveShardData(args %+v)", kv.gid, kv.me, args)
	defer func() { DPrintf("[gid %d][server %d] ReceiveShardData(args %+v, reply %+v)", kv.gid, kv.me, args, reply) }()

	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	// 过滤需要的 k-v 对数据存放到本地
	shards := make([]int, 0)
	for shard, gid := range kv.isReceivingFrom {
		if gid == args.Gid {
			for k, v := range args.Data {
				if key2shard(k) == shard {
					kv.Data[k] = v
				}
			}
			shards = append(shards, shard)
		}
	}
	// 标记 gid 对应的数据接收完成
	for _, shard := range shards {
		kv.isReceivingFrom[shard] = InvalidGid
	}
	// 返回成功
	reply.Ok = true
}

func (kv *ShardKV) startReconfiguration(config1 shardctrler.Config, config2 shardctrler.Config) {
	DPrintf("[gid %d][server %d] startReconfiguration(Config Num %v -> Num %v)", kv.gid, kv.me, config1.Num, config2.Num)

	isReceivingFrom := [shardctrler.NShards]int{}
	sendDataTo := make([][]string, 0)
	for i := 0; i < shardctrler.NShards; i ++ {
		gid1 := config1.Shards[i]
		gid2 := config2.Shards[i]
		if gid1 != gid2 {
			if kv.gid == gid1 {
				// 当前服务器所在的组失去了 shard i，需要给 gid2 发送数据
				sendDataTo = append(sendDataTo, cloneStringSlice(config2.Groups[gid2]))
			} else if kv.gid == gid2 {
				// 当前服务器所在的组得到了 shard i
				isReceivingFrom[i] = gid1
			} else {
				continue
			}
		}
	}

	// TODO:(...)
	kv.isReceivingFrom = isReceivingFrom

	_, isLeader := kv.rf.GetState()
	// leader 在组中同步
	if isLeader {
		data := cloneData(kv.Data)

		for _, servers := range sendDataTo {
			op := Op{
				OpType: Reconfigure,
				Servers: cloneStringSlice(servers),
				Data: cloneData(data),	
			}
			kv.rf.Start(op)
		}
	}
}

func (kv *ShardKV) queryConfig() {
	for !kv.killed() {
		newConfig := kv.sc.Query(-1)
		kv.mu.Lock()
		config := kv.config
		if config.Num != newConfig.Num {
			// 配置发生改变
			// 更新配置
			// 如果改变了就会使得当前组立刻拒绝执行所有跟失去 shard 相关的请求
			kv.config = newConfig
			kv.startReconfiguration(config, newConfig)
		}
		kv.mu.Unlock()


		time.Sleep(QueryConfigDuration * time.Millisecond)
	}
}

// 从 applyCh 中读取已经提交的命令
func (kv *ShardKV) applier() {
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

func (kv *ShardKV) execOp(op Op, index int) {
	DPrintf("[gid %d][server %d] execOp(op %+v, index %v)", kv.gid, kv.me, op, index)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.LastApplied + 1 != index {
		panic(fmt.Sprintf("[gid %d][server %d] kv.lastApplied %v + 1 != index %v", kv.gid, kv.me, kv.LastApplied, index))
	}
	kv.LastApplied ++

	if op.OpType == Reconfigure {
		// 发送 RPC 时不应该阻塞当前协程
		go kv.execReconfiguration(op)
		return
	}

	v, ok := kv.LastOp[op.ClientId]
	// 这条命令已经执行过
	if ok && v.CallId >= op.CallId {
		return
	}
	// 这个 key 不是当前组负责的，不要执行
	if !kv.checkRightGroup(op.Key) {
		return
	}

	lop := LastOperation{CallId: op.CallId}
	// 执行操作
	switch op.OpType {
	case Get:
		v, ok := kv.Data[op.Key]
		if ok {
			lop.Result = v
		} else {
			lop.Result = ""
		}
	case Put:
		kv.Data[op.Key] = op.Value
	case Append:
		value, ok := kv.Data[op.Key]
		if !ok {
			value = ""
		}
		kv.Data[op.Key] = value + op.Value
	default:
		DPrintf("[gid %d][server %d] execOp(op %+v) error: op.OpType %v", kv.gid, kv.me, op, op.OpType)
	}

	// 记录最新操作
	kv.LastOp[op.ClientId] = &lop
	// 通知 RPC 协程
	kv.notifyCond.Broadcast()
}

// 给 op 对应的服务器集群发送数据
func (kv *ShardKV) execReconfiguration(op Op) {
	args := ReceiveShardDataArgs{Gid: kv.gid, Data: cloneData(op.Data)}
	for _, server := range op.Servers {
		for {
			var reply ReceiveShardDataReply
			ok := kv.makeCall(server, "ShardKV.ReceiveShardData", &args, &reply)
			if ok && reply.Ok {
				break
			}
			time.Sleep(ClerkCallDuration * time.Millisecond)
		}
	}
}

func (kv *ShardKV) makeSnapshot() {
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

			DPrintf("[gid %d][server %d] makeSnapshot(): LastApplied %v", kv.gid, kv.me, LastApplied)
			kv.rf.Snapshot(LastApplied, w.Bytes())
		}
		time.Sleep(SnapshotDuration * time.Millisecond)
	}
}

func (kv *ShardKV) applySnapshot(snapshotIndex int, snapshot []byte) {
	DPrintf("[gid %d][server %d] applySnapshot(snapshotIndex %v)", kv.gid, kv.me, snapshotIndex)

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

func (kv *ShardKV) restoreState(snapshot []byte) {
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
		log.Printf("[gid %d][server %d] restoreState error!", kv.gid, kv.me)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.LastApplied = lastApplied
	kv.Data = data
	kv.LastOp = lastOp
}

func (kv *ShardKV) makeCall(serverName string, methodName string, args interface{}, reply interface{}) bool {
	ch := make(chan bool, 1)
	srv := kv.make_end(serverName)
	go func() {
		ch <- srv.Call(methodName, args, reply)
	}()
	select {
	case ok := <-ch:
		return ok
	case <-time.After(ClerkRPCTimeout * time.Millisecond):
		return false
	}
}

func cloneStringSlice(a []string) []string {
	b := make([]string, len(a))
	copy(b, a)
	return b
}

func cloneData(a map[string]string) map[string]string {
	b := make(map[string]string)
	for k, v := range a {
		b[k] = v
	}
	return b
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	d := atomic.LoadInt32(&kv.dead)
	return d == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	DPrintf("[gid %d][server %d] StartKVServer()", gid, me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.Data = make(map[string]string)
	kv.sc = shardctrler.MakeClerk(kv.ctrlers)
	kv.notifyCond = sync.NewCond(&kv.mu)
	kv.LastOp = make(map[int64]*LastOperation)
	kv.LastApplied = 0
	kv.persister = persister
	kv.isReceivingFrom = [shardctrler.NShards]int{}

	// 恢复到保存的快照状态
	kv.restoreState(kv.persister.ReadSnapshot())

	go kv.applier()

	if maxraftstate != -1 {
		go kv.makeSnapshot()
	}

	go kv.queryConfig()

	return kv
}
