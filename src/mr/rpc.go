package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type ReqTask struct {

}

// task 完成后返回的结构体
type Reply struct {
	// map task / reduce task 的下标
	Index 		int
	// 这次任务的事务 ID
	TxnID		TransactionID
	// map task 返回的中间文件名集合
	Filenames	[]string
}

// 事务 ID，用来唯一标识某次 worker 处理事务的过程
type TransactionID int64
type Task struct {
	IsMapTask	bool
	Filenames 	[]string
	Index		int
	NReduce 	int
	TxnID		TransactionID
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
