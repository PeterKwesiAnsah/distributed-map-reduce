package mr

const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
	WaitTask   TaskType = 2
	ExitTask   TaskType = 3
)

type TaskId uint32
type TaskType uint32
type Args uint32
type WorkerId uint32
type WorkerInfo struct {
	Sockname string
	Tasks    Tasks
}

type TaskStatus bool
type JobStatus bool

type WorkerConfig struct {
	MapTasksCount    uint32
	ReduceTasksCount uint32
}
type Chunk struct {
	Path     string
	Sockname string
	Index    uint32
}
type Task struct {
	FileChunks []Chunk
	TaskId     TaskId
	Type       TaskType
}

type TaskReport struct {
	TaskId TaskId
	Type   TaskType
}

type NotifyStatus int32
type NotifyTaskOutput struct {
	ReduceTasks []string
}
