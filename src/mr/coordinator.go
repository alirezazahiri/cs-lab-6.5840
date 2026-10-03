package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const taskTimeout = 10 * time.Second

type taskState int

const (
	taskIdle taskState = iota
	taskRunning
	taskCompleted
)

type task struct {
	state      taskState
	startedAt  time.Time
	generation int
	file       string
}

type Coordinator struct {
	mu sync.Mutex

	mapTasks    []task
	reduceTasks []task
	nReduce     int
	mapDone     bool
	done        bool
}

// RequestTask assigns an available task, or asks the worker to wait or exit.
func (c *Coordinator) RequestTask(
	args *RequestTaskArgs,
	reply *RequestTaskReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.expireTasksLocked()

	if !c.mapDone {
		for id := range c.mapTasks {
			t := &c.mapTasks[id]
			if t.state == taskIdle {
				t.state = taskRunning
				t.startedAt = time.Now()
				t.generation++

				*reply = RequestTaskReply{
					Type:       MapTask,
					TaskID:     id,
					File:       t.file,
					NReduce:    c.nReduce,
					NMap:       len(c.mapTasks),
					Generation: t.generation,
				}
				return nil
			}
		}

		reply.Type = WaitTask
		return nil
	}

	if !c.done {
		for id := range c.reduceTasks {
			t := &c.reduceTasks[id]
			if t.state == taskIdle {
				t.state = taskRunning
				t.startedAt = time.Now()
				t.generation++

				*reply = RequestTaskReply{
					Type:       ReduceTask,
					TaskID:     id,
					NReduce:    c.nReduce,
					NMap:       len(c.mapTasks),
					Generation: t.generation,
				}
				return nil
			}
		}

		reply.Type = WaitTask
		return nil
	}

	reply.Type = ExitTask
	return nil
}

// ReportTask records a task completion if it belongs to the current attempt.
func (c *Coordinator) ReportTask(
	args *ReportTaskArgs,
	reply *ReportTaskReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("--> Coordinator received report for %v task %d (gen %d)", args.Type, args.TaskID, args.Generation)

	var tasks []task
	switch args.Type {
	case MapTask:
		tasks = c.mapTasks
	case ReduceTask:
		tasks = c.reduceTasks
	default:
		return nil
	}

	if args.TaskID < 0 || args.TaskID >= len(tasks) {
		return nil
	}

	t := &tasks[args.TaskID]
	log.Printf("--> Task %d state: %v, generation: %d vs received: %d", args.TaskID, t.state, t.generation, args.Generation)

	if t.state == taskRunning && t.generation == args.Generation {
		t.state = taskCompleted
		reply.Accepted = true

		if args.Type == MapTask {
			c.mapDone = allTasksCompleted(c.mapTasks)
		} else {
			c.done = allTasksCompleted(c.reduceTasks)
		}
	} else {
		log.Printf("--> Task %d report REJECTED! (state=%v, expected gen=%d)", args.TaskID, t.state, t.generation)
	}

	return nil
}

// expireTasksLocked makes timed-out attempts available for reassignment.
func (c *Coordinator) expireTasksLocked() {
	now := time.Now()

	for i := range c.mapTasks {
		t := &c.mapTasks[i]
		if t.state == taskRunning && now.Sub(t.startedAt) > taskTimeout {
			t.state = taskIdle
		}
	}

	for i := range c.reduceTasks {
		t := &c.reduceTasks[i]
		if t.state == taskRunning && now.Sub(t.startedAt) > taskTimeout {
			t.state = taskIdle
		}
	}
}

func allTasksCompleted(tasks []task) bool {
	for i := range tasks {
		if tasks[i].state != taskCompleted {
			return false
		}
	}
	return true
}

// server starts the RPC server used by workers.
func (c *Coordinator) server(sockname string) {
	if err := rpc.Register(c); err != nil {
		log.Fatalf("rpc register error: %v", err)
	}
	rpc.HandleHTTP()

	_ = os.Remove(sockname)
	listener, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatalf("listen error %s: %v", sockname, err)
	}

	go func() {
		if err := http.Serve(listener, nil); err != nil {
			log.Printf("RPC server stopped: %v", err)
		}
	}()
}

// Done reports whether all map and reduce tasks have completed.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// MakeCoordinator creates a coordinator and starts its RPC server.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	if nReduce < 0 {
		log.Fatalf("nReduce must not be negative")
	}

	c := &Coordinator{
		mapTasks:    make([]task, len(files)),
		reduceTasks: make([]task, nReduce),
		nReduce:     nReduce,
	}

	for i, filename := range files {
		c.mapTasks[i].file = filename
	}

	c.mapDone = len(files) == 0
	c.done = c.mapDone && nReduce == 0

	c.server(sockname)
	return c
}
