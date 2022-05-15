package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type JobState int

const (
	Mapping JobState = iota
	Reducing
	Done
)

type Coordinator struct {
	// Your definitions here.
	State        JobState
	MappedTaskId map[int]struct{}
	MapTasks     []*MapTask
	ReduceTasks  []*ReduceTask
	Mu           sync.Mutex
	MaxTaskId    int
	NReduce      int
	WorkerSum    int
	RWMu         sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
const TimeOut = 10 * time.Second

func (c *Coordinator) StartTask(_ *PlaceHolder, reply *Task) error {
	reply.Operation = TaskWait

	if c.State == Mapping {
		for _, task := range c.MapTasks {
			now := time.Now()
			c.Mu.Lock()
			if task.State == Running && task.StartTime.Add(TimeOut).Before(now) {
				task.State = Idle
			}
			if task.State == Idle {
				task.StartTime = now
				task.State = Running
				task.Id = c.MaxTaskId
				c.MaxTaskId++
				c.Mu.Unlock()
				log.Printf("assigned map task %d %s", task.Id, task.Filename)

				reply.Operation = TaskRun
				reply.IsMap = true
				reply.NReduce = c.NReduce
				reply.Map = *task
				return nil
			}
			c.Mu.Unlock()
		}
	} else if c.State == Reducing {
		for _, task := range c.ReduceTasks {
			now := time.Now()
			c.Mu.Lock()
			if task.State == Running && task.StartTime.Add(TimeOut).Before(now) {
				task.State = Idle
			}
			if task.State == Idle {
				task.StartTime = now
				task.State = Running
				task.InterMediaFile = nil
				for id, _ := range c.MappedTaskId {
					task.InterMediaFile = append(task.InterMediaFile, fmt.Sprintf("mr-%d-%d", id, task.Id))
				}
				c.Mu.Unlock()
				log.Printf("assigned reduce task %d", task.Id)

				reply.Operation = TaskRun
				reply.IsMap = false
				reply.NReduce = c.NReduce
				reply.Reduce = *task
				return nil
			}
			c.Mu.Unlock()
		}
	}
	return nil
}
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) Finished(args *FinishArgs, _ *PlaceHolder) error {
	if args.IsMap {
		for _, task := range c.MapTasks {
			if task.Id == args.Id {
				c.RWMu.RLock()
				task.State = Finished
				c.RWMu.RUnlock()
				log.Printf("finished task %d, total %d", task.Id, len(c.MapTasks))
				c.MappedTaskId[task.Id] = struct{}{}
				break
			}
		}
		//
		for _, t := range c.MapTasks {
			if t.State != Finished {
				return nil
			}
		}
		c.RWMu.RLock()
		c.State = Reducing
		c.RWMu.RUnlock()
	} else {
		for _, task := range c.ReduceTasks {
			if task.Id == args.Id {
				task.State = Finished
				break
			}
		}
		//
		for _, t := range c.ReduceTasks {
			if t.State != Finished {
				return nil
			}
		}
		c.RWMu.RLock()
		c.State = Done
		c.RWMu.RUnlock()
	}
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
	c.RWMu.Lock()
	defer c.RWMu.Unlock()
	var judge bool = c.State == Done

	// Your code here.

	return judge
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:        Mapping,
		NReduce:      nReduce,
		MaxTaskId:    0,
		MappedTaskId: make(map[int]struct{}),
	}
	for _, file := range files {
		c.MapTasks = append(c.MapTasks, &MapTask{TaskMeta: TaskMeta{State: Idle}, Filename: file})
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &ReduceTask{TaskMeta: TaskMeta{State: Idle, Id: i}})
	}

	// Your code here.

	c.server()
	return &c
}
