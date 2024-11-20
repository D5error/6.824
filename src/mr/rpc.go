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

type AssignTaskArgs struct {
}
type AssignTaskReply struct {
	TaskType TaskType
	MapTask *MapTask
	ReduceTask *ReduceTask
}

type FinishMapTaskArgs struct {
	TaskId int
}
type FinishMapTaskReply struct {
}

type FinishReduceTaskArgs struct {
	TaskId int
}
type FinishReduceTaskReply struct {
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
