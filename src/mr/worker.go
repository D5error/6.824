package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for reduce: sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {
	for {
		args := AssignTaskArgs{}
		reply := AssignTaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			fmt.Printf("call AssignTask failed!\n")
			return
		}
		if reply.TaskType == WAITING {
			continue
		} else if reply.TaskType == MAPPING {
			doMapTask(reply.MapTask, mapFunction)
		} else if reply.TaskType == REDUCING {
			doReduceTask(reply.ReduceTask, reduceFunction)
		} else if reply.TaskType == FINISH {
			return
		}
			time.Sleep(100 * time.Microsecond)
	}
}

func doMapTask(mapTask *MapTask, mapFunction func(string, string) []KeyValue) {
	// read the target file
	file, err := os.Open(mapTask.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", mapTask.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapTask.FileName)
	}
	file.Close()

	// mapping
	kva := mapFunction(mapTask.FileName, string(content))

	// store to mr-X-Y
	mr_X_Y := []*os.File{}
	X := strconv.Itoa(mapTask.TaskId)
	for i := 0; i < mapTask.NReduce; i++ {
		mrXYName := "mr-" + X + "-" + strconv.Itoa(i)
		file, _ := os.Create(mrXYName)
		mr_X_Y = append(mr_X_Y, file)
	}
	for _, kv := range kva {
		i := ihash(kv.Key) % mapTask.NReduce
		enc := json.NewEncoder(mr_X_Y[i])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode key-value")
		}
	}
	for _, f := range mr_X_Y {
		f.Close()
	}

	callFinishMapTask(mapTask)
}

func doReduceTask(reduceTask *ReduceTask, reduceFunction func(string, []string) string) {
	result := []KeyValue{}
	Y := strconv.Itoa(reduceTask.TaskId)

	// read the mr_X_Y
	for X := 0; X < reduceTask.NMap; X++ {
		mrXYName := "mr-" + strconv.Itoa(X) + "-" + Y
		mr_X_Y, err := os.Open(mrXYName)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(mr_X_Y)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			result = append(result, kv)
		}
		mr_X_Y.Close()
	}

	// reducing
	sort.Sort(ByKey(result))
	outputFileName := "mr-out-" + strconv.Itoa(reduceTask.TaskId)
	outputFile, _ := os.Create(outputFileName)
	i := 0
	for i < len(result) {
		j := i + 1
		// find the first different j
		for j < len(result) && result[j].Key == result[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, result[k].Value)
		}
		output := reduceFunction(result[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", result[i].Key, output)

		i = j
	}
	outputFile.Close()

	callFinishReduceTask(reduceTask)
}

func callFinishMapTask(mapTask *MapTask) {
	args := FinishMapTaskArgs{}
	reply := FinishMapTaskReply{}
	args.TaskId = mapTask.TaskId

	ok := call("Coordinator.FinishMapTask", &args, &reply)
	if !ok {
		fmt.Printf("call FinishMapTask failed!\n")
		return
	}
}

func callFinishReduceTask(reduceTask *ReduceTask) {
	args := FinishReduceTaskArgs{}
	reply := FinishReduceTaskReply{}
	args.TaskId = reduceTask.TaskId

	ok := call("Coordinator.FinishReduceTask", &args, &reply)
	if !ok {
		fmt.Printf("call FinishReduceTask failed!\n")
		return
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":6666")
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
