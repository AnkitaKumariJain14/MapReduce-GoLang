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

type Status string
type Phase string
type Duration time.Duration

// Declaring constants for the Status of the Jobs
const (
	Ready     Status = "ready"
	In_Queue  Status = "inqueue"
	Running   Status = "running"
	Completed Status = "completed"
	Error     Status = "error"
)

// Declaring constants for Phase of Job - Map/Reduce
const (
	Map    Phase = "map"
	Reduce Phase = "reduce"
)

type Task struct {
	Phase             Phase  //map/reduce phase
	No_of_map_jobs    int    // total number of map jobs
	No_of_reduce_jobs int    // total number of reduce jobs
	TaskID            int    //Task ID
	File              string //File name
	Isdone            bool   // True if task done else false

}

type RequestArgs struct {
	Status_of_worker bool //true if worker is alive else false
}

type RequestReply struct {
	Task         Task //Details of the Task
	Is_task_done bool // Flag to check if done
}

type ReportArgs struct {
	Status_of_worker bool
	TaskID           int
	Isdone           bool
}

type ReportReply struct {
	Coord_ACK bool // True if Co-ordinator response was processed succesfully
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

func coordinatorSock() string {
	s := "/var/tmp/cs612-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
