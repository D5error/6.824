package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// task's type
type TaskType int

// task's type
const (
	MAPPING TaskType = iota
	REDUCING
	WAITING
	FINISH
)

// task's state
type TaskState int

// task's state
const (
	UNDO TaskState = iota
	DOING
	DONE
)

type MapTask struct {
	rwMutex  sync.RWMutex
	TaskId   int
	FileName string
	NReduce  int
	state    TaskState
}
type ReduceTask struct {
	rwMutex sync.RWMutex
	TaskId  int
	NMap    int
	state   TaskState
}

type Coordinator struct {
	rwMutex     sync.RWMutex
	state       TaskType
	mapTasks    []*MapTask
	reduceTasks []*ReduceTask
	taskTimeOut time.Duration
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.rwMutex.RLock()
	if c.state == MAPPING {
		for _, mapTask := range c.mapTasks {
			mapTask.rwMutex.RLock()
			if mapTask.state == UNDO {
				mapTask.rwMutex.RUnlock()
				mapTask.rwMutex.Lock()
				mapTask.state = DOING
				reply.TaskType = MAPPING
				reply.MapTask = mapTask
				// fmt.Printf("coordinator: assign map task %d\n", mapTask.TaskId)
				go c.checkMapTaskRunningTime(mapTask)
				mapTask.rwMutex.Unlock()
				break
			}
			reply.TaskType = WAITING
			mapTask.rwMutex.RUnlock()
		}
	} else if c.state == REDUCING {
		for _, reduceTask := range c.reduceTasks {
			reduceTask.rwMutex.RLock()
			if reduceTask.state == UNDO {
				reduceTask.rwMutex.RUnlock()
				reduceTask.rwMutex.Lock()
				reduceTask.state = DOING
				reply.TaskType = REDUCING
				reply.ReduceTask = reduceTask
				// fmt.Printf("coordinator: assign reduce task %d\n", reduceTask.TaskId)
				go c.checkReduceTaskRunningTime(reduceTask)
				reduceTask.rwMutex.Unlock()
				break
			}
			reply.TaskType = WAITING
			reduceTask.rwMutex.RUnlock()
		}
	} else {
		reply.TaskType = FINISH
	}
	c.rwMutex.RUnlock()
	return nil
}

func (c *Coordinator) FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) error {
	hasMapped := true
	for _, mapTask := range c.mapTasks {
		mapTask.rwMutex.RLock()
		if args.TaskId == mapTask.TaskId {
			mapTask.rwMutex.RUnlock()
			mapTask.rwMutex.Lock()
			mapTask.state = DONE
			// fmt.Printf("coordinator: finish map task %d\n", mapTask.TaskId)
			mapTask.rwMutex.Unlock()
			mapTask.rwMutex.RLock()
		}
		if mapTask.state != DONE {
			hasMapped = false
		}
		mapTask.rwMutex.RUnlock()
	}

	if hasMapped {
		c.rwMutex.Lock()
		c.state = REDUCING
		c.rwMutex.Unlock()
	}
	return nil
}

func (c *Coordinator) FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) error {
	hasReduced := true
	for _, reduceTask := range c.reduceTasks {
		reduceTask.rwMutex.RLock()
		if args.TaskId == reduceTask.TaskId {
			reduceTask.rwMutex.RUnlock()
			reduceTask.rwMutex.Lock()
			reduceTask.state = DONE
			// fmt.Printf("coordinator: finish reduce task %d\n", reduceTask.TaskId)
			reduceTask.rwMutex.Unlock()
			reduceTask.rwMutex.RLock()
		}
		if reduceTask.state != DONE {
			hasReduced = false
		}
		reduceTask.rwMutex.RUnlock()
	}

	if hasReduced {
		c.rwMutex.Lock()
		c.state = FINISH
		c.rwMutex.Unlock()
	}
	return nil
}

// check the map task is/isn't timeout
func (c *Coordinator) checkMapTaskRunningTime(mapTask *MapTask) {
	time.Sleep(c.taskTimeOut)
	mapTask.rwMutex.RLock()
	if mapTask.state == DOING {
		mapTask.rwMutex.RUnlock()
		// fmt.Printf("coordinator: map task %d timed out, assign it again!\n", mapTask.TaskId)
		mapTask.rwMutex.Lock()
		mapTask.state = UNDO
		mapTask.rwMutex.Unlock()
		return
	}
	mapTask.rwMutex.RUnlock()
}

// check the reduce task is/isn't timeout
func (c *Coordinator) checkReduceTaskRunningTime(reduceTask *ReduceTask) {
	time.Sleep(c.taskTimeOut)
	reduceTask.rwMutex.RLock()
	if reduceTask.state == DOING {
		reduceTask.rwMutex.RUnlock()
		// fmt.Printf("coordinator: reduce task %d timed out, assign it again!\n", reduceTask.TaskId)
		reduceTask.rwMutex.Lock()
		reduceTask.state = UNDO
		reduceTask.rwMutex.Unlock()
		return
	}
	reduceTask.rwMutex.RUnlock()
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	// fmt.Println("coordinator: running...")
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":6666")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.rwMutex.RLock()
	if c.state == FINISH {
		ret = true
	}
	c.rwMutex.RUnlock()

	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// init
	c.mapTasks = make([]*MapTask, 0)
	c.reduceTasks = make([]*ReduceTask, 0)
	c.state = MAPPING
	c.taskTimeOut = 10 * time.Second

	// create map tasks
	for i, fileName := range files {
		mapTask := &MapTask{
			TaskId:   i,
			FileName: fileName,
			NReduce:  nReduce,
			state:    UNDO,
		}
		c.mapTasks = append(c.mapTasks, mapTask)
	}

	// create reduce tasks
	for i := 0; i < nReduce; i++ {
		reduceTask := &ReduceTask{
			TaskId: i,
			NMap:   len(files),
			state:  UNDO,
		}
		c.reduceTasks = append(c.reduceTasks, reduceTask)
	}

	// run
	c.server()
	return &c
}
