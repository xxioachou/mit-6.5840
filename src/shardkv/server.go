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
	ErrChangeGroup
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
}

type LastOperation struct {
	CallId 	int64
	Result 	string
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
	Data				map[string]string		 	// 存储的数据（k-v对）
	sc          		*shardctrler.Clerk       	// 分片控制器
	notifyChan  		map[int]chan interface{} 	// 用于通知 RPC 协程操作完成(index->chan)
	LastOp      		map[int64]LastOperation 	// clientId -> last operation，用来处理重复的 RPC 操作，实现幂等性
	LastApplied 		int                      	// 最近收到的已经提交的日志索引
	persister 			*raft.Persister

	Config				shardctrler.Config		 			// 最近收到的 config
	MyShards			map[int]struct{}		 			// 维护当前服务器应该服务哪些 shard 的集合
	DataForMigration  	map[int]map[int]map[string]string	// 需要迁移的数据(configNum, shard -> data)
	ShardDataReqs       map[int]int       					// 后台协程会从中读取数据发送 RPC(shard->configNum)
}

func (kv *ShardKV) checkRightGroup(key string) bool {
	_, ok := kv.MyShards[key2shard(key)]
	return ok
}

// 原子地获取 index 对应的管道
func (kv *ShardKV) index2notifyChan(index int) chan interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.notifyChan[index]; !ok {
		kv.notifyChan[index] = make(chan interface{}, 1)
	}
	return kv.notifyChan[index]
}

// 判断操作是否相同
func equalOp(a, b interface{}) bool {
	op1, ok1 := a.(Op)
	op2, ok2 := b.(Op)
	if ok1 && ok2 {
		return op1.OpType == op2.OpType && op1.ClientId == op2.ClientId && op2.CallId == op2.CallId
	}

	cfg1, ok1 := a.(shardctrler.Config)
	cfg2, ok2 := b.(shardctrler.Config)
	if ok1 && ok2 {
		return cfg1.Num == cfg2.Num
	}

	msr1, ok1 := a.(MigrateShardReply)
	msr2, ok2 := b.(MigrateShardReply)
	if ok1 && ok2 {
		return msr1.ConfigNum == msr2.ConfigNum && msr1.Shard == msr2.Shard
	}

	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[%d:%d] Get(args %+v)", kv.gid, kv.me, args)
	defer func() { DPrintf("[%d:%d] Get(args %+v), reply %+v", kv.gid, kv.me, args, reply) }()
	// Your code here.

	kv.mu.Lock()
	rightGroup := kv.checkRightGroup(args.Key)
	kv.mu.Unlock()
	if !rightGroup {
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{
		OpType: Get,
		ClientId: args.Identifier.ClientId,
		CallId: args.Identifier.CallId,
		Key: args.Key,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	currOp := <-kv.index2notifyChan(index)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !equalOp(op, currOp) {
		if c, ok := currOp.(Op); ok && c.OpType == ErrChangeGroup {
			// 可能得到请求的时候还在这一组，但是数据从 Raft 层返回的时候不在了
			reply.Err = ErrWrongGroup
		} else {
			// 同一个下标不同的命令，拒绝请求
			reply.Err = ErrWrongLeader
		}
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
	DPrintf("[%d:%d] PutAppend(args %+v)", kv.gid, kv.me, args)
	defer func() { DPrintf("[%d:%d] PutAppend(args %+v), reply %+v", kv.gid, kv.me, args, reply) }()
	// Your code here.
	var opType OpType
	if args.Op == "Put" {
		opType = Put
	} else if args.Op == "Append" {
		opType = Append
	} else {
		DPrintf("[%d:%d] Unknow op %v", kv.gid, kv.me, args.Op)
		return
	}

	kv.mu.Lock()
	rightGroup := kv.checkRightGroup(args.Key)
	kv.mu.Unlock()
	if !rightGroup {
		DPrintf("[%d:%d] PutAppend(args %+v) branch 1", kv.gid, kv.me, args)
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
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("[%d:%d] PutAppend(args %+v) branch 2", kv.gid, kv.me, args)
		reply.Err = ErrWrongLeader
		return
	}

	currOp := <-kv.index2notifyChan(index)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !equalOp(op, currOp) {
		if c, ok := currOp.(Op); ok && c.OpType == ErrChangeGroup {
			DPrintf("[%d:%d] PutAppend(args %+v) branch 3", kv.gid, kv.me, args)
			// 可能得到请求的时候还在这一组，但是数据从 Raft 层// 同一个下标不同的命令，拒绝请求返回的时候不在了
			reply.Err = ErrWrongGroup
		} else {
			// 同一个下标不同的命令，拒绝请求
			reply.Err = ErrWrongLeader
		}
	} else {
		v, ok := kv.LastOp[args.Identifier.ClientId]
		if !ok || v.CallId != args.Identifier.CallId {
			DPrintf("[%d:%d] PutAppend(args %+v) branch 4", kv.gid, kv.me, args)
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	}
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	DPrintf("[%d:%d] MigrateShard(args %+v)", kv.gid, kv.me, args)
	defer func() { DPrintf("[%d:%d] MigrateShard(args %+v, reply %+v)", kv.gid, kv.me, args, reply) }()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.ErrWrongLeader = true
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ConfigNum > kv.Config.Num {
		reply.ErrWrongLeader = true
		return
	}

	// 没有对应的数据准备
	if _, ok := kv.DataForMigration[args.ConfigNum]; ok {
		if _, ok := kv.DataForMigration[args.ConfigNum][args.Shard]; ok {
			reply.ErrWrongLeader = false
		} else {
			reply.ErrWrongLeader = true
			return
		}
	} else {
		reply.ErrWrongLeader = true
		return
	}

	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
	reply.Data, reply.LastOperation = kv.cloneDataAndLastOperation(args.ConfigNum, args.Shard)

	// delete(kv.DataForMigration[args.ConfigNum], args.Shard)
	// if len(kv.DataForMigration[args.ConfigNum]) == 0 {
	// 	delete(kv.DataForMigration, args.ConfigNum)
	// }
}

func (kv *ShardKV) cloneDataAndLastOperation(cfgNum int, shard int) (map[string]string, map[int64]LastOperation) {
	data := make(map[string]string)
	lastOperation := make(map[int64]LastOperation)
	for k, v := range kv.DataForMigration[cfgNum][shard] {
		data[k] = v
	}
	for k, v := range kv.LastOp {
		lastOperation[k] = v
	}
	return data, lastOperation
}

// 周期性地检查配置有没有改变
func (kv *ShardKV) queryConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		// 注意每次只同步一个配置，前一个配置没有处理完成就不要拿新数据（因为 ctrler 的客户端不支持并发）
		if _, isLeader := kv.rf.GetState(); isLeader && len(kv.ShardDataReqs) == 0 {
			config := kv.sc.Query(kv.Config.Num + 1)
			// 配置发生改变，leader 开始同步
			if config.Num == kv.Config.Num + 1 {
				kv.rf.Start(config)
			}
		}
		kv.mu.Unlock()

		time.Sleep(QueryConfigDuration * time.Millisecond)
	}
}

// leader 遍历需要发送的 RPC map，发送 RPC 获取数据，并开始同步
func (kv *ShardKV) tryReqShardData() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			if len(kv.ShardDataReqs) > 0 {
				DPrintf("[%d:%d] tryReqShardData(): kv.shardDataReqs %+v", kv.gid, kv.me, kv.ShardDataReqs)
				var wg sync.WaitGroup
				for shard, cfgNum := range kv.ShardDataReqs {
					wg.Add(1)
					go func(shard int, config shardctrler.Config) {
						// DPrintf("[%d:%d] tryReqShardData(): shard %v, config %+v", kv.gid, kv.me, shard, config)
						defer wg.Done()
						args := MigrateShardArgs {
							ConfigNum: config.Num,
							Shard: shard,
						}
						for _, server := range config.Groups[config.Shards[shard]] {
							var reply MigrateShardReply
							if kv.makeCall(server, "ShardKV.MigrateShard", &args, &reply) && !reply.ErrWrongLeader {
								kv.rf.Start(reply)
								return
							}
						}
					}(shard, kv.sc.Query(cfgNum))
				}
				kv.mu.Unlock()
				wg.Wait()
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(ReqShardDataDuration * time.Millisecond)
	}
}

// 从 applyCh 中读取已经提交的命令
func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if m.CommandValid {
			// 提交了命令
			op := m.Command
			kv.execCommand(op, m.CommandIndex)
		} else if m.SnapshotValid {
			// 提交了日志
			kv.applySnapshot(m.SnapshotIndex, m.Snapshot)
		}
	}
}

func (kv *ShardKV) execCommand(command interface{}, index int) {
	kv.mu.Lock()
	defer func() { 	kv.index2notifyChan(index) <- command }()
	defer kv.mu.Unlock()
	if kv.LastApplied + 1 != index {
		panic(fmt.Sprintf("[%d:%d] kv.lastApplied %v + 1 != index %v", kv.gid, kv.me, kv.LastApplied, index))
	}
	kv.LastApplied ++

	if c, ok :=  command.(shardctrler.Config); ok {
		// 应用配置
		kv.execApplyConfig(c)
		return
	}
	if m, ok := command.(MigrateShardReply); ok {
		// 应用迁移的数据
		kv.execMigrationDataSync(m)
		return
	}

	op := command.(Op)
	v, ok := kv.LastOp[op.ClientId]
	// 这条命令已经执行过
	if ok && v.CallId >= op.CallId {
		DPrintf("[%d:%d] execCommand(command %+v, index %v): error command executed", kv.gid, kv.me, command, index)
		return
	}
	// 这个 key 不是当前组负责的，不要执行
	if !kv.checkRightGroup(op.Key) {
		DPrintf("[%d:%d] execCommand(command %+v, index %v): error not right group", kv.gid, kv.me, command, index)
		op.OpType = ErrChangeGroup
		command = op
		return
	}

	DPrintf("[%d:%d] execCommand(command %+v, index %v)", kv.gid, kv.me, command, index)
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
		DPrintf("[%d:%d] execOp(op %+v) error: op.OpType %v", kv.gid, kv.me, op, op.OpType)
	}

	// 记录最新操作
	kv.LastOp[op.ClientId] = lop
}

func (kv *ShardKV) execApplyConfig(newConfig shardctrler.Config) {
	if newConfig.Num <= kv.Config.Num {
		DPrintf("[%d:%d] newConfig %v <= kv.Config %v", kv.gid, kv.me, newConfig, kv.Config)
		return
	}
	
	oldConfig := kv.Config
	lostShards := make([]int, 0)
	DPrintf("[%d:%d] execApplyConfig(newConfig %+v): oldConfig %+v", kv.gid, kv.me, newConfig, oldConfig)
	for i := 0; i < shardctrler.NShards; i ++ {
		gid1 := oldConfig.Shards[i]
		gid2 := newConfig.Shards[i]
		if gid1 != gid2 && (gid1 == kv.gid || gid2 == kv.gid) {
			if gid1 == kv.gid {
				lostShards = append(lostShards, i)
			} else {
				if oldConfig.Num != 0 {
					// 需要向别人请求
					kv.ShardDataReqs[i] = oldConfig.Num
				} else {
					kv.MyShards[i] = struct{}{}
				}	
			}
		}
	}
	if len(lostShards) > 0 {
		DPrintf("[%d:%d] kv.MyShards before %v", kv.gid, kv.me, kv.MyShards)
		for _, shard := range lostShards {
			delete(kv.MyShards, shard)
			data := make(map[string]string)
			for k, v := range kv.Data {
				if key2shard(k) == shard {

					data[k] = v
					delete(kv.Data, k)
				}
			}			
			if _, ok := kv.DataForMigration[oldConfig.Num]; !ok {
				kv.DataForMigration[oldConfig.Num] = make(map[int]map[string]string)
			}
			kv.DataForMigration[oldConfig.Num][shard] = data
			// DPrintf("[%d:%d] execApplyConfig(): lost shard %v, data %v", kv.gid, kv.me, shard, data)
		}
		DPrintf("[%d:%d] kv.MyShards after %v", kv.gid, kv.me, kv.MyShards)
	}
	// 更新配置
	kv.Config = newConfig
}	


func (kv *ShardKV) execMigrationDataSync(migrationData MigrateShardReply) {
	DPrintf("[%d:%d] execMigrationDataSync(migrationData %+v)", kv.gid, kv.me, migrationData)
	if migrationData.ConfigNum != kv.Config.Num - 1 {
		DPrintf("[%d:%d] execMigrationDataSync(migrationData %+v): migrationData.ConfigNum %v != kv.config.Num %v - 1", kv.gid, kv.me, migrationData, migrationData.ConfigNum, kv.Config.Num)
		return
	}
	delete(kv.ShardDataReqs, migrationData.Shard)

	if _, ok := kv.MyShards[migrationData.Shard]; !ok {
		kv.MyShards[migrationData.Shard] = struct{}{}
		for k, v := range migrationData.Data {
			kv.Data[k] = v
		}
		for k, v := range migrationData.LastOperation {
			if lop, ok := kv.LastOp[k]; !ok || lop.CallId <= v.CallId {
				kv.LastOp[k] = v
			}
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

			e.Encode(kv.Config)
			e.Encode(kv.MyShards)
			e.Encode(kv.DataForMigration)
			e.Encode(kv.ShardDataReqs)
			kv.mu.Unlock()

			DPrintf("[%d:%d] makeSnapshot(): LastApplied %v", kv.gid, kv.me, LastApplied)
			kv.rf.Snapshot(LastApplied, w.Bytes())
		}
		time.Sleep(SnapshotDuration * time.Millisecond)
	}
}

func (kv *ShardKV) applySnapshot(snapshotIndex int, snapshot []byte) {
	DPrintf("[%d:%d] applySnapshot(snapshotIndex %v)", kv.gid, kv.me, snapshotIndex)

	w := bytes.NewBuffer(snapshot)
	e := labgob.NewDecoder(w)
	var index int
	data := make(map[string]string)
	lastOp := make(map[int64]LastOperation)

	config := shardctrler.Config{}
	myshards := make(map[int]struct{})
	dataForMigration := make(map[int]map[int]map[string]string)
	shardDataReqs := make(map[int]int)

	if  e.Decode(&index) != nil ||
		e.Decode(&data) != nil ||
		e.Decode(&lastOp) != nil ||
		e.Decode(&config) != nil ||
		e.Decode(&myshards) != nil ||
		e.Decode(&dataForMigration) != nil ||
		e.Decode(&shardDataReqs) != nil {
		log.Fatalf("[%d:%d] applySnapshot() error!", kv.gid, kv.me)
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

	kv.Config = config
	kv.MyShards = myshards
	kv.DataForMigration = dataForMigration
	kv.ShardDataReqs = shardDataReqs
}

func (kv *ShardKV) restoreState(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var lastApplied int
	data := make(map[string]string)
	lastOp := make(map[int64]LastOperation)

	config := shardctrler.Config{}
	myshards := make(map[int]struct{})
	dataForMigration := make(map[int]map[int]map[string]string)
	shardDataReqs := make(map[int]int)

	w := bytes.NewBuffer(snapshot)
	e := labgob.NewDecoder(w)
	if e.Decode(&lastApplied) != nil ||
		e.Decode(&data) != nil || 
		e.Decode(&lastOp) != nil ||
		e.Decode(&config) != nil ||
		e.Decode(&myshards) != nil ||
		e.Decode(&dataForMigration) != nil ||
		e.Decode(&shardDataReqs) != nil {
		log.Printf("[%d:%d] restoreState error!", kv.gid, kv.me)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.LastApplied = lastApplied
	kv.Data = data
	kv.LastOp = lastOp

	kv.Config = config
	kv.MyShards = myshards
	kv.DataForMigration = dataForMigration
	kv.ShardDataReqs = shardDataReqs
}

func (kv *ShardKV) makeCall(serverName string, methodName string, args interface{}, reply interface{}) bool {
	DPrintf("[%d:%d] makeCall(serverName %v, methodName %v, args %+v)", kv.gid, kv.me, serverName, methodName, args)
	ch := make(chan bool, 1)
	srv := kv.make_end(serverName)
	go func() {
		ch <- srv.Call(methodName, args, reply)
	}()
	select {
	case ok := <-ch:
		// DPrintf("[%d:%d] makeCall(serverName %v, methodName %v, args %+v): reply %+v", kv.gid, kv.me, serverName, methodName, args, reply)
		return ok
	case <-time.After(ClerkRPCTimeout * time.Millisecond):
		// DPrintf("[%d:%d] makeCall(serverName %v, methodName %v, args %+v): no reply", kv.gid, kv.me, serverName, methodName, args)
		return false
	}
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
	DPrintf("[%d:%d] StartKVServer()", gid, me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateShardReply{})

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
	kv.notifyChan = make(map[int]chan interface{})
	kv.LastOp = make(map[int64]LastOperation)
	kv.LastApplied = 0
	kv.persister = persister

	kv.MyShards = make(map[int]struct{})
	kv.DataForMigration = make(map[int]map[int]map[string]string)
	kv.ShardDataReqs = make(map[int]int)

	// 恢复到保存的快照状态
	kv.restoreState(kv.persister.ReadSnapshot())

	go kv.applier()

	if maxraftstate != -1 {
		go kv.makeSnapshot()
	}

	go kv.queryConfig()
	go kv.tryReqShardData()

	return kv
}
