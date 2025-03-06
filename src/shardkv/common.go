package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

const InvalidServer = -1
const InvalidGid = 0
const ClerkRPCTimeout = 1000
const ClerkCallDuration = 100
const QueryConfigDuration = 100
const SnapshotDuration = 10
const ReqShardDataDuration = 100

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   		string
	Value 		string
	Op    		string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Identifier 	Identifier
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key 		string
	// You'll have to add definitions here.
	Identifier 	Identifier
}

type GetReply struct {
	Err   Err
	Value string
}

type Identifier struct {
	ClientId		int64
	CallId			int64
}

type MigrateShardArgs struct {
	ConfigNum		int
	Shard 			int
}

type MigrateShardReply struct {
	ConfigNum		int
	Shard			int
	ErrWrongLeader	bool
	Data			map[string]string
	LastOperation	map[int64]LastOperation		// 用来给请求去重
}
