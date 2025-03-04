package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	dead 	int32
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs			[]Config 					// indexed by config num
	notifyCond		*sync.Cond					// 用于通知 RPC 协程操作完成
	lastOp			map[int64]*LastOperation	// clientId -> *last operation，用来处理重复的 RPC 操作
	lastApplied		int							// 最近收到的已经提交的日志索引

}	

type LastOperation struct {
	CallId 			int64	
	Config 			Config
}

type OpType int
const (
	Join = iota
	Leave
	Move
	Query
)

type Op struct {
	// Your data here.
	OpType 			OpType
	Args 			interface{}					// JoinArgs, LeaveArgs ...
}

// 检查是否需要继续等待(任期改变或者 index 对应的操作被提交，返回 false)
func (sc *ShardCtrler) check(startTerm int, index int) bool {
	currentTerm, _ := sc.rf.GetState()
	return currentTerm == startTerm && sc.lastApplied < index
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	DPrintf("[server %d] Join(args %+v)", sc.me, args)
	defer func() { DPrintf("[server %d] Join(args %+v), reply %+v", sc.me, args, reply) }()
	// Your code here.
	// 检查是否已经执行，如果已经执行填入 reply
	if sc.checkExecuted(args.Identifier.ClientId, args.Identifier.CallId, reply, Join) {
		return
	}

	op := Op{
		OpType: Join,
		Args: *args,
	}
	index, startTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	
	
	sc.mu.Lock()
	// 阻塞等待，直到任期改变或者 index 对应的操作被提交
	for sc.check(startTerm, index) {
		sc.notifyCond.Wait()
	}
	defer sc.mu.Unlock()

	currentTerm, _ := sc.rf.GetState()
	if currentTerm != startTerm {
		reply.WrongLeader = true
	} else {
		v, ok := sc.lastOp[args.Identifier.ClientId]
		if !ok || v.CallId != args.Identifier.CallId {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	DPrintf("[server %d] Leave(args %+v)", sc.me, args)
	defer func() { DPrintf("[server %d] Leave(args %+v), reply %+v", sc.me, args, reply) }()
	// Your code here.
	// 检查是否已经执行，如果已经执行填入 reply
	if sc.checkExecuted(args.Identifier.ClientId, args.Identifier.CallId, reply, Leave) {
		return
	}

	op := Op{
		OpType: Leave,
		Args: *args,
	}
	index, startTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	
	
	sc.mu.Lock()
	// 阻塞等待，直到任期改变或者 index 对应的操作被提交
	for sc.check(startTerm, index) {
		sc.notifyCond.Wait()
	}
	defer sc.mu.Unlock()

	currentTerm, _ := sc.rf.GetState()
	if currentTerm != startTerm {
		reply.WrongLeader = true
	} else {
		v, ok := sc.lastOp[args.Identifier.ClientId]
		if !ok || v.CallId != args.Identifier.CallId {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	DPrintf("[server %d] Move(args %+v)", sc.me, args)
	defer func() { DPrintf("[server %d] Move(args %+v), reply %+v", sc.me, args, reply) }()
	// Your code here.
	// 检查是否已经执行，如果已经执行填入 reply
	if sc.checkExecuted(args.Identifier.ClientId, args.Identifier.CallId, reply, Move) {
		return
	}

	op := Op{
		OpType: Move,
		Args: *args,
	}
	index, startTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	
	
	sc.mu.Lock()
	// 阻塞等待，直到任期改变或者 index 对应的操作被提交
	for sc.check(startTerm, index) {
		sc.notifyCond.Wait()
	}
	defer sc.mu.Unlock()

	currentTerm, _ := sc.rf.GetState()
	if currentTerm != startTerm {
		reply.WrongLeader = true
	} else {
		v, ok := sc.lastOp[args.Identifier.ClientId]
		if !ok || v.CallId != args.Identifier.CallId {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	DPrintf("[server %d] Query(args %+v)", sc.me, args)
	defer func() { DPrintf("[server %d] Query(args %+v), reply %+v", sc.me, args, reply) }()
	// Your code here.
	// 检查是否已经执行，如果已经执行填入 reply
	if sc.checkExecuted(args.Identifier.ClientId, args.Identifier.CallId, reply, Query) {
		return
	}

	op := Op{
		OpType: Query,
		Args: *args,
	}
	index, startTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	
	
	sc.mu.Lock()
	// 阻塞等待，直到任期改变或者 index 对应的操作被提交
	for sc.check(startTerm, index) {
		sc.notifyCond.Wait()
	}
	defer sc.mu.Unlock()

	currentTerm, _ := sc.rf.GetState()
	if currentTerm != startTerm {
		reply.WrongLeader = true
	} else {
		v, ok := sc.lastOp[args.Identifier.ClientId]
		if !ok || v.CallId != args.Identifier.CallId {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Config = v.Config
		}
	}
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
	DPrintf("[server %d] Kill", sc.me)
}

func (sc *ShardCtrler) killed() bool {
	x := atomic.LoadInt32(&sc.dead)
	return x == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// 检查某次 RPC 调用对应的操作是否已经执行
func (sc *ShardCtrler) checkExecuted(clientId, callId int64, reply interface{}, opType OpType) bool {
	sc.mu.Lock()
	v, ok := sc.lastOp[clientId]
	sc.mu.Unlock()
	if !ok || v.CallId < callId {
		// 没有执行	
		return false
	}


	eq := v.CallId == callId

	switch opType {
	case Join:
		reply.(*JoinReply).WrongLeader = !eq
	case Leave:
		reply.(*LeaveReply).WrongLeader = !eq
	case Move:
		reply.(*MoveReply).WrongLeader = !eq
	case Query:
		if eq {
			reply.(*QueryReply).WrongLeader = false
			reply.(*QueryReply).Config = v.Config
		} else {
			reply.(*QueryReply).WrongLeader = true
		}
	default:
		DPrintf("[server %d] checkExecuted(opType %v)", sc.me, opType)
	}

	return true
}

// 从 applyCh 中读取已经提交的命令
func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		if m.CommandValid {		
			// 提交了命令
			sc.mu.Lock()
			if sc.lastApplied + 1 != m.CommandIndex {
				panic(fmt.Sprintf("sc.lastApplied %v + 1 != m.CommandIndex %v", sc.lastApplied, m.CommandIndex))
			}
			sc.lastApplied ++
			sc.mu.Unlock()
			
			op := m.Command.(Op)
			sc.execOp(op)
		}
	}
}

// 执行操作，将结果存放到 results, 唤醒对应的 RPC handler
func (sc *ShardCtrler) execOp(op Op) {
	switch op.OpType {
	case Join:
		sc.execJoin(op.Args.(JoinArgs))
	case Leave:
		sc.execLeave(op.Args.(LeaveArgs))
	case Move:
		sc.execMove(op.Args.(MoveArgs))
	case Query:
		sc.execQuery(op.Args.(QueryArgs))
	default:
		DPrintf("[server %d] execOp(op %+v) error: op.OpType %v", sc.me, op, op.OpType)
	}
}

func (sc *ShardCtrler) execJoin(args JoinArgs) {
	DPrintf("[server %d] execJoin(args %+v)", sc.me, args)
	sc.mu.Lock()
	v, ok := sc.lastOp[args.Identifier.ClientId]
	sc.mu.Unlock()
	// 这条命令已经执行过
	if ok && v.CallId >= args.Identifier.CallId {
		return
	}
	
	lop := LastOperation{CallId: args.Identifier.CallId}

	// 执行操作
	sc.mu.Lock()
	Num := len(sc.configs)
	config := sc.configs[Num - 1]
	sc.mu.Unlock()

	newConfig := Config{
		Num: Num,
		Shards: config.Shards,
		Groups: cloneMap(config.Groups),
	}
	for gid, names := range args.Servers {
		newConfig.Groups[gid] = names
	}
	sc.makeShardBalance(&newConfig)
	
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 添加配置
	sc.configs = append(sc.configs, newConfig)
	// 记录最新操作
	sc.lastOp[args.Identifier.ClientId] = &lop
	// 通知 RPC 协程
	sc.notifyCond.Broadcast()
}

func (sc *ShardCtrler) execLeave(args LeaveArgs) {
	DPrintf("[server %d] execLeave(args %+v)", sc.me, args)
	sc.mu.Lock()
	v, ok := sc.lastOp[args.Identifier.ClientId]
	sc.mu.Unlock()
	// 这条命令已经执行过
	if ok && v.CallId >= args.Identifier.CallId {
		return
	}
	
	lop := LastOperation{CallId: args.Identifier.CallId}
	// 执行操作
	sc.mu.Lock()
	Num := len(sc.configs)
	config := sc.configs[Num - 1]
	sc.mu.Unlock()
	
	newConfig := Config{
		Num: Num,
		Shards: config.Shards,
		Groups: cloneMap(config.Groups),
	}
	for _, gid := range args.GIDs {
		delete(newConfig.Groups, gid)
	}
	sc.makeShardBalance(&newConfig)

	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 添加配置
	sc.configs = append(sc.configs, newConfig)
	// 记录最新操作
	sc.lastOp[args.Identifier.ClientId] = &lop
	// 通知 RPC 协程
	sc.notifyCond.Broadcast()
}

func (sc *ShardCtrler) execMove(args MoveArgs) {
	DPrintf("[server %d] execMove(args %+v)", sc.me, args)
	sc.mu.Lock()
	v, ok := sc.lastOp[args.Identifier.ClientId]
	sc.mu.Unlock()
	// 这条命令已经执行过
	if ok && v.CallId >= args.Identifier.CallId {
		return
	}
	
	lop := LastOperation{CallId: args.Identifier.CallId}
	// 执行操作
	sc.mu.Lock()
	Num := len(sc.configs)
	config := sc.configs[Num - 1]
	sc.mu.Unlock()

	newConfig := Config{
		Num: Num,
		Shards: config.Shards,
		Groups: cloneMap(config.Groups),
	}
	newConfig.Shards[args.Shard] = args.GID

	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 添加配置
	sc.configs = append(sc.configs, newConfig)
	// 记录最新操作
	sc.lastOp[args.Identifier.ClientId] = &lop
	// 通知 RPC 协程
	sc.notifyCond.Broadcast()
}

func (sc *ShardCtrler) execQuery(args QueryArgs) {
	DPrintf("[server %d] execQuery(args %+v)", sc.me, args)
	sc.mu.Lock()
	v, ok := sc.lastOp[args.Identifier.ClientId]
	sc.mu.Unlock()
	// 这条命令已经执行过
	if ok && v.CallId >= args.Identifier.CallId {
		return
	}
	
	lop := LastOperation{CallId: args.Identifier.CallId}
	// 执行操作
	sc.mu.Lock()
	if args.Num == -1 || args.Num >= len(sc.configs) {
		lop.Config = sc.configs[len(sc.configs) - 1]
	} else {
		lop.Config = sc.configs[args.Num]
	}
	sc.mu.Unlock()


	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 记录最新操作
	sc.lastOp[args.Identifier.ClientId] = &lop
	// 通知 RPC 协程
	sc.notifyCond.Broadcast()
}

func (sc *ShardCtrler) makeShardBalance(config *Config) {
	DPrintf("[server %d] makeShardBalance(config %+v)", sc.me, config)
	cnt := make(map[int]int)
	groupNum := len(config.Groups)
	if groupNum == 0 {
		for i := range config.Shards {
			config.Shards[i] = InvalidGid
		}
		return
	}

	x := NShards / groupNum
	r := NShards % groupNum
	shards := make([]int, 0)

	// 找出多余的 shards
	for i, gid := range config.Shards {
		_, ok := config.Groups[gid]

		if ok {
			if _, ok := cnt[gid]; !ok {
				cnt[gid] = 0
			}
			if cnt[gid] >= x {
				if cnt[gid] == x {
					if r == 0 {
						shards = append(shards, i)
					} else {
						cnt[gid] ++
						r --
					}
				} else {
					shards = append(shards, i)
				}
			} else {
				cnt[gid] ++
			}
		} else {
			shards = append(shards, i)
		}
	}
	
	t := make([][]int, 0)
	for gid := range config.Groups {
		i, ok := cnt[gid]
		if !ok {
			i = 0
		}
		t = append(t, []int{gid, i})
	}
	sort.SliceStable(t, func(i, j int) bool {
		return t[i][0] < t[j][0]
	})
	// 把多余的 shards 分配给较少的 gid
	for _, v := range t {
		gid, i := v[0], v[1]
		for i < x {
			config.Shards[shards[len(shards) - 1]] = gid
			shards = shards[:len(shards) - 1]
			i ++
		}
		if i == x && r > 0 {
			config.Shards[shards[len(shards) - 1]] = gid
			shards = shards[:len(shards) - 1]
			r --
		}
	}
}

func cloneStringSlice(a []string) []string {
	b := make([]string, len(a))
	copy(b, a)
	return b
}

func cloneMap(a map[int][]string) map[int][]string {
	b := make(map[int][]string)
	for k, v := range a {
		b[k] = cloneStringSlice(v)
	}
	return b
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})

	sc.notifyCond = sync.NewCond(&sc.mu)
	sc.lastOp = make(map[int64]*LastOperation)
	sc.lastApplied = 0

	go sc.applier()

	return sc
}
