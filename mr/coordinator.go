package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type workerKey string
type workerState struct {
	sockName string
	lastSeen time.Time
	tasks    Tasks
	alive    bool
}

type Tasks struct {
	//non-negative (pending/waiting/non-finished reduceTask), negative idle
	Current                       int
	CurrentRPartitionToMapOutDone Bitmap
	Received                      Bitmap
	Status                        Bitmap
}

type Coordinator struct {
	mu               sync.Mutex
	taskToWorkerKeys *[][]workerKey
	workerKeyToTasks map[workerKey]workerState
	scheduled        Bitmap
	status           Bitmap
	jobStatus        JobStatus
}

type workerChannelItem struct {
	info   WorkerInfo
	taskId TaskId
}

var reduceTasksCnt uint32
var mapTasksCnt uint32
var sockName string

func (c *Coordinator) pingWorkers(workers []workerKey) {
	for _, worker := range workers {
		go worker.pingWorker(c)
	}
}

func (wt workerKey) pingWorker(c *Coordinator) {
	client, err := connectrpc(string(wt))
	if err != nil {
		return
	}
	defer client.Close()
	timeout := time.After(1 * time.Second)
	done := make(chan *rpc.Call, 1)
	tasks := new(Tasks)
	client.Go("WorkerTaskState.HeartbeatPing", wt, tasks, done)
	select {
	case <-timeout:
		return
	case t := <-done:
		tasks := *(t.Reply.(*Tasks))
		c.mu.Lock()
		ws := (c.workerKeyToTasks[wt])
		ws.alive = true
		ws.lastSeen = time.Now()
		c.workerKeyToTasks[wt] = ws
		c.status.Add(tasks.Status)
		c.mu.Unlock()
	}

}
func (wt *workerState) receievedAnyMapTask() bool {
	for i := range mapTasksCnt {
		if wt.tasks.Received.Get(i) {
			return true
		}
	}
	return false
}

func (c *Coordinator) ReportTaskFailed(arg TaskReport, reply *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	bitIndex := uint32(arg.TaskId)
	if arg.Type == ReduceTask {
		bitIndex += mapTasksCnt
	}
	c.scheduled.Clear(bitIndex)
	c.status.Clear(bitIndex)
	return nil
}

func (c *Coordinator) GetWorkerConfig(arg WorkerInfo, reply *WorkerConfig) error {
	*reply = WorkerConfig{
		MapTasksCount:    mapTasksCnt,
		ReduceTasksCount: reduceTasksCnt,
	}
	return nil
}

// idle/non-active/nil -> in_progress
func (c *Coordinator) GetTask(arg WorkerInfo, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.jobStatus {
		*reply = Task{Type: ExitTask}
		return nil
	}
	c.workerKeyToTasks[workerKey(arg.Sockname)] = workerState{
		lastSeen: time.Now(),
		tasks:    arg.Tasks,
		alive:    true,
		sockName: arg.Sockname,
	}

	// extract this into a separate func
	for i := range mapTasksCnt {
		if !c.scheduled.Get(i) {
			sockName := os.Args[1]
			files := os.Args[2:]
			taskToWorkerKeys := *c.taskToWorkerKeys // (need sep lock)
			if taskToWorkerKeys[i] == nil {
				taskToWorkerKeys[i] = []workerKey{workerKey(arg.Sockname)}
				*reply = Task{
					TaskId: (TaskId(i)),
					FileChunks: []Chunk{
						{Sockname: sockName, Path: files[i], Index: i},
					},
					Type: MapTask,
				}
				c.scheduled.Set(i)
				return nil
			}
			taskToWorkerKeys[i] = append(taskToWorkerKeys[i], workerKey(arg.Sockname))
			*reply = Task{
				TaskId: (TaskId(i)),
				FileChunks: []Chunk{
					{Sockname: sockName, Path: files[i], Index: i},
				},
				Type: MapTask,
			}
			c.scheduled.Set(i)
			return nil
		}
	}

	//check for any pending reduceTask
	if arg.Tasks.Current > 0 {
		var todoMapOutputs []uint32
		var chunks []Chunk
		for i := range mapTasksCnt {
			if !arg.Tasks.CurrentRPartitionToMapOutDone.Get(i) {
				todoMapOutputs = append(todoMapOutputs, i)
			}
		}
		//scan workers
		for _, worker := range c.workerKeyToTasks {
			if !worker.alive {
				continue
			}
			// recieved any mapTasks??
			if worker.receievedAnyMapTask() {
				for _, mapOutput := range todoMapOutputs {
					// worker alive, received task, processed task
					if worker.tasks.Received.Get(mapOutput) && worker.tasks.Status.Get(mapOutput) {
						chunks = append(chunks, Chunk{
							Index:    mapOutput,
							Sockname: worker.sockName,
							Path:     fmt.Sprintf("mr-%d-%d", mapOutput, arg.Tasks.Current),
						})
					}

				}
			}
		}
		if len(chunks) == 0 {
			*reply = Task{Type: WaitTask}
			return nil
		}
		*reply = Task{
			TaskId:     TaskId(arg.Tasks.Current),
			FileChunks: chunks,
			Type:       ReduceTask,
		}
		return nil
	}

	for reduceTask := range reduceTasksCnt {
		if !c.scheduled.Get(reduceTask + mapTasksCnt) {
			var chunks []Chunk
			//scan workers
			for _, worker := range c.workerKeyToTasks {
				if !worker.alive {
					continue
				}
				// recieved any mapTasks??
				if worker.receievedAnyMapTask() {
					for mapOutput := range mapTasksCnt {
						// worker alive, received task, processed task
						if worker.tasks.Received.Get(mapOutput) && worker.tasks.Status.Get(mapOutput) {
							chunks = append(chunks, Chunk{
								Index:    mapOutput,
								Sockname: worker.sockName,
								Path:     fmt.Sprintf("mr-%d-%d", mapOutput, reduceTask),
							})
						}

					}
				}
			}
			*reply = Task{
				TaskId:     TaskId(reduceTask),
				FileChunks: chunks,
				Type:       ReduceTask,
			}
			c.scheduled.Set(reduceTask + mapTasksCnt)
			return nil
		}
	}
	*reply = Task{Type: WaitTask}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() JobStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range reduceTasksCnt {
		if !c.status.Get(i + mapTasksCnt) {
			return false
		}
	}
	c.jobStatus = true
	return c.jobStatus
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v\n", sockname, e)
	}
	fmt.Printf("Coordinator: Listening on %s\n", sockname)
	go http.Serve(l, nil)
}

// TODO: 3.6 (Backup Tasks)
// TODO: 4.2 (Ordering Guarantees)
// TODO: 4.3 (Combiner Function)
// TODO: 4.8 (Status Information)
// TODO: 4.9 (Counters)
// TODO: implement task locking, remove global lock

func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {

	M := (uint32)(len(files))
	reduceTasksCnt = (uint32(nReduce))
	mapTasksCnt = M

	var tasks [][]workerKey = make([][]workerKey, M)
	c := Coordinator{
		taskToWorkerKeys: &tasks,
		scheduled:        NewBitmap((mapTasksCnt + reduceTasksCnt)),
		workerKeyToTasks: make(map[workerKey]workerState),
	}
	c.server(sockname)

	go func(c *Coordinator) {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			//create a slice of socknames
			var alive []workerKey
			c.mu.Lock()
			for k, w := range c.workerKeyToTasks {
				if !w.alive {
					continue
				}
				alive = append(alive, k)
			}
			c.mu.Unlock()
			go c.pingWorkers(alive)
		}
	}(&c)

	go func(c *Coordinator) {
		ticker := time.NewTicker(2 * time.Second)
		for range ticker.C {
			c.mu.Lock()
			for k, w := range c.workerKeyToTasks {
				if w.alive {
					//continue
					deadline := 10 * time.Second
					if time.Since(w.lastSeen) > deadline {
						ws := (c.workerKeyToTasks[k])
						ws.alive = false
						//We are handling only one M piece for a worker. This will change for backup tasks where
						//multiple workers race to complete an M piece
						for task := range mapTasksCnt + reduceTasksCnt {
							if task < mapTasksCnt {
								if ws.tasks.Received.Get(task) {
									c.status.Clear(task)
									c.scheduled.Set(task)
								}
							}
							if task < reduceTasksCnt {
								if ws.tasks.Received.Get(task+mapTasksCnt) && !ws.tasks.Status.Get(task+mapTasksCnt) {
									c.status.Clear(task + mapTasksCnt)
									c.scheduled.Set(task + mapTasksCnt)
								}
							}

						}
						c.workerKeyToTasks[k] = ws
					}
				}
			}
			c.mu.Unlock()
		}

	}(&c)

	return &c
}
