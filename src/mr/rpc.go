package mr

//
// RPC definitions.
//

// TaskType identifies the kind of work a worker should perform.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// RequestTaskArgs is sent by a worker asking for work.
type RequestTaskArgs struct{}

// RequestTaskReply describes a task assigned by the coordinator.
type RequestTaskReply struct {
	Type       TaskType
	TaskID     int
	File       string
	NReduce    int
	NMap       int
	Generation int
}

// ReportTaskArgs identifies a task that a worker has completed.
type ReportTaskArgs struct {
	Type       TaskType
	TaskID     int
	Generation int
}

// ReportTaskReply indicates whether the coordinator accepted the report.
type ReportTaskReply struct {
	Accepted bool
}
