package kvsrv

import (
	"log"
	"strconv"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu		sync.Mutex

	// Your definitions here.
	// 存储的数据
	pairs	map[string]string
	// 为每个调用保存的结果
	results map[string]string
}

func getKey(clientID, callID int64) string {
	return strconv.FormatInt(clientID, 10) + "," + strconv.FormatInt(callID, 10)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// // 1. 检查是否执行过了
	// key := getKey(args.ClientID, args.CallID)
	// if v, ok := kv.results[key]; ok {
	// 	reply.Value = v
	// 	return
	// }

	// 2. 执行
	reply.Value = ""
	if v, ok := kv.pairs[args.Key]; ok {
		reply.Value = v
	}

	// // 3. 插入 results
	// kv.results[key] = reply.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 1. 检查是否执行过了
	key := getKey(args.ClientID, args.CallID)
	if v, ok := kv.results[key]; ok {
		reply.Value = v
		return
	}

	// 2. 执行
	kv.pairs[args.Key] = args.Value
	reply.Value = ""

	// 3. 插入 results
	kv.results[key] = reply.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	// 1. 检查是否执行过了
	key := getKey(args.ClientID, args.CallID)
	if v, ok := kv.results[key]; ok {
		reply.Value = v
		return
	}

	// 2. 执行
	// same as old value
	reply.Value = ""
	if v, ok := kv.pairs[args.Key]; ok {
		reply.Value = v
	}
	kv.pairs[args.Key] = reply.Value + args.Value

	// 3. 插入结果
	kv.results[key] = reply.Value
}

func (kv *KVServer) Succeed(args *SucceedArgs, reply *SucceedReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := getKey(args.ClientID, args.CallID)
	delete(kv.results, key)
	*reply = true
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.pairs = make(map[string]string)
	kv.results = make(map[string]string)

	return kv
}
