package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	mu			sync.Mutex
	// 用于阻塞直到存在状态为 IDLE 的 map task
	mapTaskCond				= sync.NewCond(&mu)
	// 用于阻塞直到所有 map task 都是 COMPLETED
	mapTaskCompletedcond	= 	sync.NewCond(&mu)
	// 用于阻塞直到存在状态为 IDLE 的 reduce task
	reduceTaskCond			= sync.NewCond(&mu)
)
const (
	TIMEOUTLIMIT = 10
)

type Status int
const (
	IDLE = iota
	INPROGRESS
	COMPLETED
)

type transaction struct {
	txnID		TransactionID
	beginTime	time.Time
}

type Coordinator struct {
	// Your definitions here.
	NReduce					int
	MapTasksCompletedCnt	int
	ReduceTasksCompletedCnt	int
	// 在任务被分发出去时使用
	NextTxnID				TransactionID
	MapTasks				[]Task
	ReduceTasks				map[int]*Task
	// 下标 -> Status
	MapTaskStatus			map[int]Status
	ReduceTaskStatus		map[int]Status
	// 记录 INPROGRESS 任务的处理事务
	// 下标 -> transaction
	MTaskProcessingTxn		map[int]transaction
	RTaskProcessingTxn 		map[int]transaction
	// 状态为 IDLE 的 map task 队列
	IdleMapTask				[]int
	// 状态为 IDLE 的 reduce task 队列
	IdleReduceTask			[]int
}

func (c *Coordinator) CloneTask(dest, src *Task) {
	dest.IsMapTask = src.IsMapTask
	dest.Filenames = make([]string, len(src.Filenames))
	copy(dest.Filenames, src.Filenames)
	dest.Index = src.Index
	dest.NReduce = src.NReduce
}

// 获取状态为 IDLE 的 map task
func (c *Coordinator) GetMapTask(task *Task) bool {
	mu.Lock()
	defer mu.Unlock()

	// 1. 阻塞直到存在 IDLE 的 map task
	for len(c.IdleMapTask) == 0 && c.MapTasksCompletedCnt < len(c.MapTasks) {
		mapTaskCond.Wait()
	}

	// 2. 检查是否全部 map task 都完成了
	if c.MapTasksCompletedCnt >= len(c.MapTasks) {
		return false
	}

	if len(c.IdleMapTask) == 0 {
		panic("Expect c.IdleMapTask to have element(s).")
	}

	// 3. 取出队头 map task
	i := c.IdleMapTask[0]
	c.IdleMapTask = c.IdleMapTask[1:]

	// 4. 标记 map task 状态为 INPROGRESS，并复制 task 内容
	c.MapTaskStatus[i] = INPROGRESS
	c.CloneTask(task, &c.MapTasks[i])

	// 5. 记录这次任务被处理的事务
	task.TxnID = c.NextTxnID
	c.NextTxnID ++
	c.MTaskProcessingTxn[i] = transaction{txnID: task.TxnID, beginTime: time.Now()}

	return true
}

// 获取状态为 IDLE 的 reduce task
func (c *Coordinator) GetReduceTask(task *Task) bool {
	mu.Lock()
	defer mu.Unlock()
	// 1. 阻塞直到所有 map task 都是 COMPLETED 状态
	for c.MapTasksCompletedCnt < len(c.MapTasks) {
		mapTaskCompletedcond.Wait()
	}

	// 2. 阻塞直到 c.IdleReduceTask 不为空
	for len(c.IdleReduceTask) == 0 && 
		c.ReduceTasksCompletedCnt < len(c.ReduceTasks){
		reduceTaskCond.Wait()
	}
	// 3. 检查是否所有 reduce task 都完成了
	if c.ReduceTasksCompletedCnt >= len(c.ReduceTasks) {
		return false
	}

	if len(c.IdleReduceTask) == 0 {
		panic("Expectd c.IdleReduceTask to have element(s).")
	}

	// 4. 取出队列头
	i := c.IdleReduceTask[0]
	c.IdleReduceTask = c.IdleReduceTask[1:]

	// 5. 改变任务状态并复制任务内容
	c.ReduceTaskStatus[i] = INPROGRESS
	c.CloneTask(task, c.ReduceTasks[i])

	// 6. 记录这次任务被分配出去的事务
	task.TxnID = c.NextTxnID
	c.NextTxnID ++
	c.RTaskProcessingTxn[i] = transaction{txnID: task.TxnID, beginTime: time.Now()}

	return true
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(req *ReqTask, reply *Task) error {
	if ok := c.GetMapTask(reply); ok {
		// log.Printf("a map task is distributed to worker, index is %d, txnID is %d.", reply.Index, reply.TxnID)
		return nil
	}

	if ok := c.GetReduceTask(reply); ok {
		// log.Printf("a reduce task is distributed to worker, index is %d, txnID is %d.", reply.Index, reply.TxnID)
		return nil
	}

	// log.Printf("tasks are all done.")

	return fmt.Errorf("tasks are all done")
}

// 添加处理完的中间文件名集合
func (c *Coordinator) MapTaskDone(req *Reply, reply *bool) error {
	mu.Lock()
	defer mu.Unlock()
	// 1. 检查这次事务是否是对应任务最后一次被分发出去的事务
	if _, ok := c.MTaskProcessingTxn[req.Index]; 
		!ok || c.MTaskProcessingTxn[req.Index].txnID != req.TxnID {
		// 可能1：这个 worker 返回太晚，这个任务已经被其他 worker 完成(INPROGRESS->IDLE->COMPLETED)
		// 可能2：这个 worker 返回太晚，这个任务重新变成 IDLE 状态(INPROGRESS->IDLE)
		// 可能3: 这个 worker 处理太慢了，超过时间限制，任务被分配给别的 worker(INPROGRESS->IDLE->INPROGRESS)
		*reply = false

		// log.Printf("unexpected txnID %d to completed the map task %d", req.TxnID, req.Index)
		return fmt.Errorf("unexpected txnID %d to completed the map task %d", req.TxnID, req.Index)
	}

	// 2. 生成中间文件名集合对应的 reduce task
	for _, filename := range req.Filenames {
		// 2.1 解析 reduce task 下标
		parts := strings.Split(filename, "-")
		idx, err := strconv.Atoi(parts[2])
		if err != nil {
			return err
		}
		// 2.2 reduce task 不存在则创建，初始状态为 IDLE，添加到 IdleReduceTask 队列
		if _, ok := c.ReduceTasks[idx]; !ok {
			c.ReduceTasks[idx] = &Task{
				IsMapTask: false,
				Filenames: make([]string, 0),
				Index: idx,
			}
			c.ReduceTaskStatus[idx] = IDLE
			c.IdleReduceTask = append(c.IdleReduceTask, idx)
		}
		// 2.3 将中间文件添加到 reduce task
		c.ReduceTasks[idx].Filenames = append(c.ReduceTasks[idx].Filenames, filename)
	}

	// 3. 标记这个 map task 已经完成
	// 3.1 改变状态
	c.MapTaskStatus[req.Index] = COMPLETED

	// 3.2 map task 完成数量加 1
	c.MapTasksCompletedCnt ++
	// 3.3 从 c.MTaskProcessingTxn 中删除对应的事务
	delete(c.MTaskProcessingTxn, req.Index)
	// 3.4 检查是否全部需要唤醒阻塞进程
	if c.MapTasksCompletedCnt >= len(c.MapTasks) {
		mapTaskCond.Broadcast()
		mapTaskCompletedcond.Broadcast()
	}

	// 4. 返回调用成功
	*reply = true
	return nil
}

// 用于告知 Coordinator 某个reduce task 已经完成
func (c *Coordinator) ReduceTaskDone(req *Reply, reply *bool) error {
	mu.Lock()
	defer mu.Unlock()
	// 1. 检查这次事务是否是对应任务最后一次被分发出去的事务
	if _, ok := c.RTaskProcessingTxn[req.Index]; 
		!ok || c.RTaskProcessingTxn[req.Index].txnID != req.TxnID {
		// 可能1：这个 worker 返回太晚，这个任务已经被其他 worker 完成(INPROGRESS->IDLE->COMPLETED)
		// 可能2：这个 worker 返回太晚，这个任务重新变成 IDLE 状态(INPROGRESS->IDLE)
		// 可能3: 这个 worker 处理太慢了，超过时间限制，任务被分配给别的 worker(INPROGRESS->IDLE->INPROGRESS)
		*reply = false
		// log.Printf("unexpected txnID %d to completed the reduce task %d", req.TxnID, req.Index)
		return fmt.Errorf("unexpected txnID %d to completed the reduce task %d", req.TxnID, req.Index)
	}

	// 2. 标记这个 reduce task 为完成
	// 2.1 改变状态
	c.ReduceTaskStatus[req.Index] = COMPLETED
	// 2.2 reduce task 完成数量加 1
	c.ReduceTasksCompletedCnt ++
	// 2.3 从 c.RTaskProcessingTxn 中删除对应的事务
	delete(c.RTaskProcessingTxn, req.Index)
	// 2.4 检查是否需要唤醒阻塞进程
	if c.ReduceTasksCompletedCnt >= len(c.ReduceTasks) {
		reduceTaskCond.Broadcast()
	}

	// 3. 返回调用成功
	*reply = true
	return nil
}

func (c *Coordinator) InspectMapTasks() {
	mu.Lock()
	defer mu.Unlock()
	// 1. 检查任务列表中 INPROGRESS 的任务
	indexs := make([]int, 0)
	for index, txn := range c.MTaskProcessingTxn {
		now := time.Now()
		// 2. 判断是否执行任务超时
		if now.Sub(txn.beginTime) > TIMEOUTLIMIT * time.Second {
			// 2.1 添加到过时列表
			indexs = append(indexs, index)
		}
	}
	// 3. 修改任务
	for _, index := range indexs {
		// log.Printf("Txn %d timeout processing map task %d", c.MTaskProcessingTxn[index].txnID, index)
		// 3.1 从 c.MTaskProcessingTxn 中删除
		delete(c.MTaskProcessingTxn, index)
		// 3.2 修改任务状态为 IDLE
		c.MapTaskStatus[index] = IDLE
		// 3.3 添加到队列
		c.IdleMapTask = append(c.IdleMapTask, index)
		// 3.4. 唤醒阻塞的分配任务进程
		mapTaskCond.Broadcast()
	}
}

func (c *Coordinator) InspectReduceTasks() {
	mu.Lock()
	defer mu.Unlock()
	// 1. 检查任务列表中 INPROGRESS 的任务
	indexs := make([]int, 0)
	for index, txn := range c.RTaskProcessingTxn {
		now := time.Now()
		// 2. 判断是否执行任务超时
		if now.Sub(txn.beginTime) > TIMEOUTLIMIT * time.Second {
			// 2.1 添加到过时列表
			indexs = append(indexs, index)
		}
	}
	// 3. 修改任务
	for _, index := range indexs {
		// log.Printf("Txn %d timeout processing reduce task %d", c.RTaskProcessingTxn[index].txnID, index)
		// 3.1 从 c.RTaskProcessingTxn 中删除
		delete(c.RTaskProcessingTxn, index)
		// 3.2 修改任务状态为 IDLE
		c.ReduceTaskStatus[index] = IDLE
		// 3.3 添加到队列
		c.IdleReduceTask = append(c.IdleReduceTask, index)
		// 3.4. 唤醒阻塞的分配任务进程
		reduceTaskCond.Broadcast()
	}
}

// 后台进程执行：每隔一秒检查任务执行是否超时
func (c *Coordinator) InspectTasks() {
	for !c.Done() {
		c.InspectMapTasks()
		c.InspectReduceTasks()
		time.Sleep(1 * time.Second)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// log.Printf("server is on %s\n", sockname)

	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.MapTasksCompletedCnt >= len(c.MapTasks) &&
		c.ReduceTasksCompletedCnt >= len(c.ReduceTasks) {
			ret = true
	}

	if ret {
		// log.Printf("tasks is all done.\n")
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// 1.1 初始化 coordinator
	c.NReduce = nReduce
	c.MapTasksCompletedCnt = 0
	c.ReduceTasksCompletedCnt = 0
	c.NextTxnID = 0
	c.MapTasks = make([]Task, 0)
	c.ReduceTasks = make(map[int]*Task)
	c.MTaskProcessingTxn = make(map[int]transaction)
	c.RTaskProcessingTxn = make(map[int]transaction)
	c.MapTaskStatus = make(map[int]Status)
	c.ReduceTaskStatus = make(map[int]Status)
	c.IdleMapTask = make([]int, 0)
	c.IdleReduceTask = make([]int, 0)
	// 1.2 生成 map task，初始状态为 IDLE，添加到 IdleTask 队列
	for i, file := range files {
		task := Task{
			IsMapTask: true, 
			Filenames: []string{file}, 
			Index: i, 
			NReduce: nReduce,
		}
		c.MapTasks = append(c.MapTasks, task)
		c.MapTaskStatus[i] = IDLE
		c.IdleMapTask = append(c.IdleMapTask, i)
	}
	// 2. 开启一个后台线程，每隔一秒检查任务的状态
	go c.InspectTasks()

	c.server()
	// log.Printf("new coordinator with %d map tasks.", len(c.MapTasks))
	return &c
}
