package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "errors"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
		// 请求任务
		reply := ReqReply{}
		reply =	ReqTask()
		if reply.TaskDone{
			break
		}
		// 执行任务
		err := doTask(mapf, reducef, reply.Task)
		if err != nil{
			ReportTask(reply.Task.TaskIndex, false)
		}else{ // TODO: 为什么这里不用else
 			ReportTask(reply.Task.TaskIndex, true)
		}
	}
	return
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}


// 请求任务
func ReqTask() ReqReply{
	// 声明参数并赋值
	args := ReqArgs{}
	args.WorkerStatus = true
	reply := ReqReply{}
	if ok := call("Coordinator.HandleReq", &args, &reply); !ok{
		log.Fatal("请求任务失败")
	}
	return reply
}

// 报告任务
func ReportTask(taskid int, state bool) ReportReply{
	// 声明参数并赋值
	args := ReportArgs{}
	args.WorkerStatus = true
	args.TaskIndex = taskid
	args.IsDone = state
	reply := ReportReply{}
	if ok := call("Coordinator.HandleReport", &args, &reply); !ok{
		log.Fatal("报告任务失败")
	}
	return reply
}

func doTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string,  task Task) error {
	if task.TaskStage == MapStage{
		err := DoMapTask(mapf, task.Filename, task.TaskIndex, task.ReduceNum)
		return err
	} else if task.TaskStage == ReduceStage {
		err := DoReduceTask(reducef, task.TaskIndex, task.MapNum)
		return err
	} else{
		log.Fatal("请求任务的任务阶段异常")
		return errors.New("请求任务的任务阶段异常")
	}
}

func intermediateName(x int, y int) string{
	return "mr-" + strconv.Itoa(x) + "-" + strconv.Itoa(y)
}

func DoMapTask(mapf func(string, string) []KeyValue, filename string, mapid int, ReduceNum int) error {
	fmt.Println("开始处理map任务...")
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for i := 0; i < ReduceNum; i++{
		// 中间输出文件名mr-X-Y
		intermediateFileName := intermediateName(mapid, i)
		fmt.Println("创建中间输出文件", intermediateFileName)
		file, _ := os.Create(intermediateFileName)
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			if ihash(kv.Key) % ReduceNum == i{
				enc.Encode(&kv)
			}
		}
		file.Close()
	}
	return nil
}

func DoReduceTask(reducef func(string, []string) string, reduceid int, MapNum int) error {
	fmt.Println("开始处理reduce任务...")
	res := make(map[string][]string)
	for i := 0; i < MapNum; i++{
		// 中间输出文件名mr-X-Y
		intermediateFileName := intermediateName(i, reduceid)
		fmt.Println("读入中间输出文件", intermediateFileName)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFileName)
		}
		// json反序列化
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			// 将v添加到k对应的字典当中
			_, ok := res[kv.Key]
			if !ok {
				res[kv.Key] = make([]string, 0)
			}
			res[kv.Key] = append(res[kv.Key], kv.Value)
		}
		file.Close()
	}
	// 提取key值用于排序
	var keys []string
	for k := range res{
		keys = append(keys, k)
	}
	// key值排序
	sort.Strings(keys)
	output_name := "mr-out-" + strconv.Itoa(reduceid)
	file, err := os.Create(output_name)
	if err != nil {
		log.Fatalf("cannot create %v", output_name)
	}
	for _, k := range keys{
		output := reducef(k, res[k])
		fmt.Fprintf(file, "%v %v\n", k, output)
	}
	file.Close()
	return nil;
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
