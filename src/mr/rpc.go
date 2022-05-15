package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type TaskState int
type TaskOperation int

const (
	Idle TaskState = iota
	Running
	Finished
)
const (
	TaskWait TaskOperation = iota
	TaskRun
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type MapTask struct {
	TaskMeta
	Filename string
}
type TaskMeta struct {
	State     TaskState
	Id        int
	StartTime time.Time
}
type ReduceTask struct {
	TaskMeta
	InterMediaFile []string
}
type Task struct {
	Operation TaskOperation
	IsMap     bool
	Map       MapTask
	Reduce    ReduceTask
	NReduce   int
}
type PlaceHolder struct {
}
type FinishArgs struct {
	IsMap bool
	Id    int
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
