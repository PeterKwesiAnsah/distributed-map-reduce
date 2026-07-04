package mr

const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
)

type TaskId uint32
type TaskType uint32
type Args uint32
type WorkerId uint32
type WorkerInfo struct {
	WorkerId WorkerId
	Sockname string
	Type     TaskType
}

type TaskStatus bool
type JobStatus bool

type Task struct {
	Path        string
	MapOutputs  []string
	TaskId      TaskId
	ReduceTasks uint32
	Type        TaskType
	Status      TaskStatus
}

type NotifyStatus int32
type NotifyTaskOutput struct {
	ReduceTasks []string
}
