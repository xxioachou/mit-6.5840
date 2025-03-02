package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID	int64
	CallID 		int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID	int64
	CallID 		int64
}

type GetReply struct {
	Err   Err
	Value string
}

// 在某个 rpc 调用成功后调用 Succeed 的参数
type SucceedArgs struct {
	ClientID 	int64
	CallID		int64
}

type SucceedReply bool