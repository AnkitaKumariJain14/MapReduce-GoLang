package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

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

	for {
		reply := RequestReply{}
		reply = reqTask()
		if reply.Is_task_done {
			break
		}
		err := assignTask(mapf, reducef, reply.Task)
		if err != nil {
			reportTask(reply.Task.TaskID, false)
		}
		reportTask(reply.Task.TaskID, true)
	}
	return
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func reqTask() RequestReply {
	args := RequestArgs{}
	args.Status_of_worker = true
	reply := RequestReply{}
	if ok := call("Coordinator.TaskRequest", &args, &reply); !ok {
		log.Fatal("The task has failed")
	}
	return reply
}

func reportTask(taskID int, isdone bool) ReportReply {
	args := ReportArgs{}
	args.Isdone = isdone
	args.TaskID = taskID
	args.Status_of_worker = true

	reply := ReportReply{}
	if ok := call("Coordinator.TaskReply", &args, &reply); !ok {
		log.Fatal("Task reporting failed")
	}
	return reply

}

func assignTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string, task Task) error {
	if task.Phase == Map {
		err := Mapjob(mapf, task.File, task.TaskID, task.No_of_reduce_jobs)
		return err
	} else if task.Phase == Reduce {
		err := Reducejob(reducef, task.No_of_map_jobs, task.TaskID)
		return err
	} else {
		log.Fatal("Return value is incorrect")
		return errors.New("return value is incorrect")
	}
}

func Mapjob(mapf func(string, string) []KeyValue, filename string, mapID int, no_of_reduce int) error {
	fmt.Println("New Map job")
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}
	file.Close()

	kva := mapf(filename, string(content))
	for i := 0; i < no_of_reduce; i++ {
		intermediateFile := inter_file(mapID, i)
		fmt.Println("Map file created")
		file, _ := os.Create(intermediateFile)
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			if ihash(kv.Key)%no_of_reduce == i {
				enc.Encode(&kv)
			}
		}
		file.Close()
	}
	return nil
}

func Reducejob(reducef func(string, []string) string, no_of_maps int, reduceID int) error {
	res := make(map[string][]string)
	for i := 0; i < no_of_maps; i++ {
		intermediateFile := inter_file(i, reduceID)
		file, err := os.Open(intermediateFile)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFile)
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := res[kv.Key]
			if !ok {
				res[kv.Key] = make([]string, 0)
			}
			res[kv.Key] = append(res[kv.Key], kv.Value)
		}

		file.Close()
	}

	var keys []string
	for k := range res {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	outputFile := outputfile(reduceID)
	fmt.Println("Reduce task done")
	outputF, _ := os.Create(outputFile)
	for _, k := range keys {
		op := reducef(k, res[k])
		fmt.Fprintf(outputF, "%v %v\n", k, op)
	}
	outputF.Close()
	return nil
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

// to create intermediate file
func inter_file(mapID int, reduceID int) string {
	var filename string
	filename = "mr" + "-" + strconv.Itoa(mapID) + "-" + strconv.Itoa(reduceID)
	return filename
}

// to create output file
func outputfile(reduceID int) string {
	var filename string
	filename = "mr-out-" + strconv.Itoa(reduceID)
	return filename
}
