package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const RPCTimeout	= 1000
const SleepDuration = 20
const InvalidServer = -1

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID	int64			
	NextCallID	int64	
	lastLeader	int			
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientID = nrand()
	ck.NextCallID = 0
	ck.lastLeader = InvalidServer

	DPrintf("[client %v] MakeClerk()", ck.ClientID)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	callID := ck.NextCallID
	ck.NextCallID ++

	args := GetArgs{Key: key, ClientID: ck.ClientID, CallID: callID}
	// succeedArgs := SucceedArgs{ClientID: ck.ClientID, CallID: callID}
	// var succeedReply SucceedReply
	DPrintf("[client %v callID %v] Get(%v)", ck.ClientID, callID, key)

	// 调用 RPC，等待直至超时或者调用成功
	call := func(server int, args GetArgs, reply *GetReply) bool {
		ch := make(chan bool, 1)
		go func() {
			ch <- ck.servers[server].Call("KVServer.Get", &args, reply)
		}()

		select {
		case ok := <-ch:
			return ok
		case <- time.After(RPCTimeout * time.Millisecond):
			return false
		}
	}

	for {
		if ck.lastLeader != InvalidServer {
			reply := GetReply{}
			// 调用成功且操作成功
			if call(ck.lastLeader, args, &reply) && reply.Err == OK {
				DPrintf("[client %v callID %v] Get(%v), reply %+v", ck.ClientID, callID, key, reply)
				return reply.Value
			}
		}

		for i := 0; i < len(ck.servers); i ++ {
			reply := GetReply{}
			// 调用成功且操作成功
			if call(i, args, &reply) && reply.Err == OK {
				ck.lastLeader = i
				DPrintf("[client %v callID %v] Get(%v), reply %+v", ck.ClientID, callID, key, reply)
				
				return reply.Value
			}
		}

		time.Sleep(SleepDuration * time.Millisecond)
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	callID := ck.NextCallID
	ck.NextCallID ++

	args := PutAppendArgs{Key: key, Value: value, ClientID: ck.ClientID, CallID: callID}
	// succeedArgs := SucceedArgs{ClientID: ck.ClientID, CallID: callID}
	// var succeedReply SucceedReply
	DPrintf("[client %v callID %v] PutAppend(key %v, value %v, op %v)", ck.ClientID, callID, key, value, op)

	// 调用 RPC，等待直至超时或者调用成功
	call := func(server int, args PutAppendArgs, reply *PutAppendReply) bool {
		ch := make(chan bool, 1)
		go func() {
			ch <- ck.servers[server].Call("KVServer." + op, &args, reply)
		}()

		select {
		case ok := <-ch:
			return ok
		case <- time.After(RPCTimeout * time.Millisecond):
			return false
		}
	}

	for {

		if ck.lastLeader != InvalidServer {
			reply := PutAppendReply{}
			if call(ck.lastLeader, args, &reply) && reply.Err == OK {
				DPrintf("[client %v callID %v] PutAppend(key %v, value %v, op %v), reply %+v", ck.ClientID, callID, key, value, op, reply)
				return
			}
		}

		
		for i := 0; i < len(ck.servers); i ++ {
			reply := PutAppendReply{}
			if call(i, args, &reply) && reply.Err == OK {
				ck.lastLeader = i
				DPrintf("[client %v callID %v] PutAppend(key %v, value %v, op %v), reply %+v", ck.ClientID, callID, key, value, op, reply)
				
				return
			}
		}

		time.Sleep(SleepDuration * time.Millisecond)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}