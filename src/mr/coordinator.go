package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus struct {
	Status    Status
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	Chan              chan Task
	Files             []string
	No_of_map_jobs    int
	No_of_reduce_jobs int
	Phase             Phase
	TaskStatus        []TaskStatus
	mu                sync.Mutex
	Isdone            bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) TaskRequest(args *RequestArgs, reply *RequestReply) error {
	if !args.Status_of_worker {
		return errors.New("The current worker is not alive")
	}
	task, flag := <-c.Chan
	if flag == true {
		reply.Task = task
		c.mu.Lock()
		c.TaskStatus[task.TaskID].Status = Running
		c.TaskStatus[task.TaskID].StartTime = time.Now()
		c.mu.Unlock()
	} else {
		reply.Is_task_done = true // returns true if no tasks are left in the queue
	}
	return nil
}

func (c *Coordinator) TaskReply(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !args.Status_of_worker {
		reply.Coord_ACK = false
		return errors.New("The current worker is not alive")
	}
	if args.Isdone == true {
		c.TaskStatus[args.TaskID].Status = Completed
	} else {
		c.TaskStatus[args.TaskID].Status = Error
	}
	reply.Coord_ACK = true
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
	ret := false
	Iscompleted := true
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, task := range c.TaskStatus {
		if task.Status == Ready {
			Iscompleted = false
			c.addTaskToQueue(id)
		} else if task.Status == In_Queue {
			Iscompleted = false
		} else if task.Status == Running {
			Iscompleted = false
			c.taskTimeout(id) //check for timeout (10 secs in our case)
		} else if task.Status == Completed {

		} else if task.Status == Error {
			Iscompleted = false
			c.addTaskToQueue(id)
		} else {
			panic("Error")
		}
	}
	// Your code here.
	if Iscompleted {
		if c.Phase == Map {
			c.initialiseReduce()
		} else {
			c.Isdone = true
			close(c.Chan)
		}
	} else {
		c.Isdone = false
	}
	ret = c.Isdone
	return ret
}

// To initialise the Reduce phase after map is done
func (c *Coordinator) initialiseReduce() {
	c.Phase = Reduce
	c.Isdone = false
	c.TaskStatus = make([]TaskStatus, c.No_of_reduce_jobs)
	for k := range c.TaskStatus {
		c.TaskStatus[k].Status = Ready
	}
}

// To add tasks to the queue (channel)
func (c *Coordinator) addTaskToQueue(taskID int) {
	c.TaskStatus[taskID].Status = In_Queue
	task := Task{
		File:              "",
		No_of_map_jobs:    len(c.Files),
		No_of_reduce_jobs: c.No_of_reduce_jobs,
		TaskID:            taskID,
		Phase:             c.Phase,
		Isdone:            false,
	}
	if c.Phase == Map {
		task.File = c.Files[taskID]
	}
	c.Chan <- task
}

// To check for timeout
func (c *Coordinator) taskTimeout(taskID int) {
	Duration := (time.Now().Sub(c.TaskStatus[taskID].StartTime))
	if Duration > (10 * time.Second) {
		c.addTaskToQueue(taskID)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Isdone = false
	c.Files = files
	c.No_of_map_jobs = len(files)
	c.No_of_reduce_jobs = nReduce
	c.Phase = Map
	c.TaskStatus = make([]TaskStatus, c.No_of_map_jobs)
	c.Chan = make(chan Task, 10)
	for id := range c.TaskStatus {
		c.TaskStatus[id].Status = Ready
	}
	// Your code here.

	c.server()
	return &c
}
