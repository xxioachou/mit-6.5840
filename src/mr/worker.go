package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		var task Task
		ok := call("Coordinator.GetTask", &ReqTask{}, &task)
		if !ok {
			// log.Println("call Coordinator.GetTask failed")
			break
		}
		// log.Printf("worker got task %v\n", task)
		if task.IsMapTask {
			ProcessMapTask(task.Filenames[0], mapf, task.Index, task.NReduce, task.TxnID)
		} else {
			ProcessReduceTask(task.Filenames, reducef, task.Index, task.TxnID)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func ProcessMapTask(filename string, mapf func(string, string) []KeyValue, index, nReduce int, txnID TransactionID) {
	// 1. 从输入文件读取内容
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// log.Printf("worker processing map task, filename %s, index %d, nReduce %d, txnID %d.\n", filename, index, nReduce, txnID)

	// 2. 调用 mapf 生成 kv 对
	kva := mapf(filename, string(content))

	iFileNames := make([]string, 0)
	files := make(map[string]*os.File)
	encoders := make(map[string]*json.Encoder)
	// 3. 将 kv 对写到文件集合
	for _, kv := range kva {
		iFileName := fmt.Sprintf("mr-%d-%d", index, ihash(kv.Key) % nReduce)
		// 3.1 文件不存在就创建这个文件
		if _, ok := files[iFileName]; !ok {
			iFile, err := os.Create(iFileName)
			if err != nil {
				log.Fatalf("cannot open %v", iFileName)
			}
			files[iFileName] = iFile
			encoders[iFileName] = json.NewEncoder(iFile)
			iFileNames = append(iFileNames, iFileName)
		}
		// log.Printf("[worker] put k-v pair %v to file %s\n", kv, iFileName)
		encoders[iFileName].Encode(&kv)
	}
	// 3.2 关闭打开的中间文件
	for _, file := range files {
		file.Close()
	}

	// 4. 返回中间文件集合给 master
	var res bool

	req := Reply{Index: index, TxnID: txnID, 
		Filenames: make([]string, len(iFileNames))}
	copy(req.Filenames, iFileNames)

	ok := call("Coordinator.MapTaskDone", &req, &res)
	if !ok || !res {
		log.Fatal("call Coordinator.MapTaskDone failed.")
	}
}

func ProcessReduceTask(finenames []string, reducef func(string, []string) string, index int, txnID TransactionID) {
	// log.Printf("worker processing reduce tasks, filenames %v, index %d, txnID %d.\n", finenames, index, txnID)
	// 1. 从文件名集合读取 kv 对
	kva := make(ByKey, 0)
	for _, filename := range finenames {
		// 1.1 打开文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file %v", filename)
		}

		// 1.2 从文件解析 kv 对
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		// 1.3 关闭文件
		file.Close()
	}
	// 2. 给 kv 对排序
	sort.Sort(kva)
	// 3. 创建结果文件
	oFileName := fmt.Sprintf("mr-out-%d", index)
	ofile, err := os.Create(oFileName)
	if err != nil {
		log.Fatalf("failed to create file %s", oFileName)
	}

	// 4. 双指针遍历 kva，调用 reducef 生成结果
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j ++
		}
		values := []string{}
		for k := i; k < j; k ++ {
			values = append(values, kva[k].Value)
		}

		// 4.1 生成结果
		output := reducef(kva[i].Key, values)

		// 4.2 写到结果文件 mr-out-index
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	// 5. 关闭结果文件
	ofile.Close()

	// 6. 告知 Coordinator reduce task 完成
	req := Reply{Index: index, TxnID: txnID}
	var res bool
	ok := call("Coordinator.ReduceTaskDone", &req, &res)
	if !ok || !res {
		log.Fatal("call Coordinator.ReduceTaskDone failed.")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
