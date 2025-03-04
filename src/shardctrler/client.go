package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.ClientID = nrand()
	ck.NextCallID = 0
	ck.lastLeader = InvalidServer

	DPrintf("[client %v] MakeClerk()", ck.ClientID)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Identifier = ck.makeIdentifier()
	args.Num = num
	DPrintf("[client %v] Query(args %+v)", ck.ClientID, args)

	for {
		if ck.lastLeader != InvalidServer {
			var reply QueryReply
			if ck.makeCall(ck.lastLeader, "ShardCtrler.Query", args, &reply) && !reply.WrongLeader {
				return reply.Config
			}
		}

		// try each known server.
		for i := range ck.servers {
			var reply QueryReply
			if ck.makeCall(i, "ShardCtrler.Query", args, &reply) && !reply.WrongLeader {
				ck.lastLeader = i
				return reply.Config
			}
		}

		time.Sleep(ClerkCallDuration * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Identifier = ck.makeIdentifier()
	args.Servers = servers
	DPrintf("[client %v] Join(args %+v)", ck.ClientID, args)

	for {
		if ck.lastLeader != InvalidServer {
			var reply JoinReply
			if ck.makeCall(ck.lastLeader, "ShardCtrler.Join", args, &reply) && !reply.WrongLeader {
				return
			}
		}

		// try each known server.
		for i := range ck.servers {
			var reply JoinReply
			if ck.makeCall(i, "ShardCtrler.Join", args, &reply) && !reply.WrongLeader {
				ck.lastLeader = i
				return
			}
		}

		time.Sleep(ClerkCallDuration * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.Identifier = ck.makeIdentifier()
	args.GIDs = gids
	DPrintf("[client %v] Leave(args %+v)", ck.ClientID, args)

	for {
		if ck.lastLeader != InvalidServer {
			var reply LeaveReply
			if ck.makeCall(ck.lastLeader, "ShardCtrler.Leave", args, &reply) && !reply.WrongLeader {
				return
			}
		}

		// try each known server.
		for i := range ck.servers {
			var reply LeaveReply
			if ck.makeCall(i, "ShardCtrler.Leave", args, &reply) && !reply.WrongLeader {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(ClerkCallDuration * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Identifier = ck.makeIdentifier()
	args.Shard = shard
	args.GID = gid
	DPrintf("[client %v] Move(args %+v)", ck.ClientID, args)

	for {
		if ck.lastLeader != InvalidServer {
			var reply MoveReply
			if ck.makeCall(ck.lastLeader, "ShardCtrler.Move", args, &reply) && !reply.WrongLeader {
				return
			}
		}
		// try each known server.
		for i := range ck.servers {
			var reply MoveReply
			if ck.makeCall(i, "ShardCtrler.Move", args, &reply) && !reply.WrongLeader {
				ck.lastLeader = i
				return
			}
		}

		time.Sleep(ClerkCallDuration * time.Millisecond)
	}
}

func (ck *Clerk) makeIdentifier() Identifier {
	id := Identifier{ClientId: ck.ClientID, CallId: ck.NextCallID}
	ck.NextCallID ++
	return id
}

func (ck *Clerk) makeCall(server int, methodName string, args interface{}, reply interface{}) bool {
	ch := make(chan bool, 1)
	go func() {
		ch <- ck.servers[server].Call(methodName, args, reply)
	}()
	select {
	case ok := <-ch:
		return ok
	case <-time.After(ClerkRPCTimeout * time.Millisecond):
		return false
	}
}