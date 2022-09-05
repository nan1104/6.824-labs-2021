
package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "errors"
import "fmt"

var MaxTaskRunTime = 2 * time.Second

// 任务状态定义
type TaskState struct{
	Status TaskStatus
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	// 任务队列
	TaskChan chan Task
	// 输入文件
	Files []string
	// map数目
	MapNum int
	// reduce数目
	ReduceNum int
	// 任务阶段
	TaskStage TaskStage
	// 任务状态
	TaskState []TaskState
	// 互斥锁
	Mutex sync.Mutex
	// 是否完成
	IsDone bool
}

// Your code here -- RPC handlers for the worker to call.
// 处理任务请求
func (c *Coordinator) HandleReq(args *ReqArgs, reply *ReqReply) error {
	fmt.Println("开始处理任务请求...")
	if !args.WorkerStatus{
		return errors.New("当前work已下线")
	}
	// 任务出队列
	task, ok := <-c.TaskChan
	if ok == true{
		reply.Task = task
		c.TaskState[task.TaskIndex].Status = TaskStatusRunning
		c.TaskState[task.TaskIndex].StartTime = time.Now()
	}else{
		reply.TaskDone = true
	}
	return nil
}
//处理任务报告
func (c *Coordinator) HandleReport(args *ReportArgs, reply *ReportReply) error {
	fmt.Println("开始处理任务报告...")
	if !args.WorkerStatus{
		reply.MasterAck = false
		return errors.New("当前work已下线")
	}
	if args.IsDone == true {
		// 任务已完成
		c.TaskState[args.TaskIndex].Status = TaskStatusDone
	} else {
		// 任务执行错误
		c.TaskState[args.TaskIndex].Status = TaskStatusError
	}
	reply.MasterAck = true
	return nil
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
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	fmt.Println("调用Done")
	ret := false

	// Your code here.
	finished := true
	c.Mutex.Lock()
	defer c.Mutex.Unlock() // 延迟到函数末尾执行
	for key, ts := range c.TaskState{
		switch ts.Status {
		case TaskStatusReady:
			// 任务就绪
			finished = false
			c.addTask(key)
		case TaskStatusQueue:
			// 任务队列中
			finished = false
		case TaskStatusRunning:
			// 任务执行中
			finished = false
			c.checkTask(key)
		case TaskStatusDone:
			// 任务完成
		case TaskStatusError:
			finished = false
			c.addTask(key)
		default:
			panic("任务状态异常...")
		}
	}
	if finished{
		// 判断阶段
		fmt.Println("finished")
		if c.TaskStage == MapStage{
			c.initReduceTask()
		} else {
			c.IsDone = true
			close(c.TaskChan)
		}
	} else {
		c.IsDone = false
	}
	ret = c.IsDone  // 这里必须是IsDone，不能是finished
	return ret
}

// 初始化reduce阶段
func (c *Coordinator) initReduceTask() {
	fmt.Println("初始化reduce阶段")
	c.TaskStage = ReduceStage
	c.IsDone = false
	c.TaskState = make([]TaskState, c.ReduceNum)
	for k:= range c.TaskState{
		c.TaskState[k].Status = TaskStatusReady
	}
	fmt.Println("初始化reduce阶段完毕")
}

// 添加task到任务池
func (c *Coordinator) addTask(taskid int) {
	var task = Task{
		TaskIndex: taskid,
		TaskStage: c.TaskStage,
		status: TaskStatusQueue,
		MapNum: c.MapNum,
		ReduceNum: c.ReduceNum,
		Filename: "",
		IsDone: false,
	}
	if c.TaskStage == MapStage{
		task.Filename = c.Files[taskid]
	}
	// 放入任务队列
	c.TaskChan <- task
}

// 检查任务运行是否超时
func (c *Coordinator) checkTask(taskid int) {
	TimeDuration := time.Now().Sub(c.TaskState[taskid].	StartTime)
	if TimeDuration > MaxTaskRunTime{
		// 超时则直接加入任务队列
		c.addTask(taskid);
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 初始化coordinator
	c.Files = files
	c.IsDone = false
	c.MapNum = len(files)
	c.ReduceNum = nReduce
	c.TaskStage = MapStage
	c.TaskState = make([]TaskState, c.MapNum)
	c.TaskChan = make(chan Task, 10)
	for k := range c.TaskState{
		c.TaskState[k].Status = TaskStatusReady
	}

	// 开启线程监听
	c.server()
	return &c
}
