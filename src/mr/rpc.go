package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

type TaskStatus string
type TaskStage string
type TimeDuration time.Duration
// 任务状态常量
const(
	TaskStatusReady TaskStatus = "ready" // 就绪
	TaskStatusQueue TaskStatus = "queue" // 队列中
	TaskStatusRunning TaskStatus = "running"  // 运行中
	TaskStatusDone TaskStatus = "done" // 运行完毕
	TaskStatusError TaskStatus = "error" // 任务错误
)
// 执行阶段常量
const(
	MapStage TaskStage = "map"
	ReduceStage TaskStage = "reduce" 
)

type Task struct{
	// 任务序号
	TaskIndex int
	// 任务阶段
	TaskStage TaskStage
	status TaskStatus
	MapNum int
	ReduceNum int
	Filename string
	IsDone bool
}

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

// Add your RPC definitions here.
 type ReqArgs struct{
	WorkerStatus bool // 当前worker是否存活	
 }
 
 type ReqReply struct{
	Task Task
	TaskDone bool // 任务池中是否还有任务可领取
 }

 type ReportArgs struct{
	WorkerStatus bool
	TaskIndex int
	IsDone bool // 任务是否完成
 }

 type ReportReply struct{
	MasterAck bool
 }


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
