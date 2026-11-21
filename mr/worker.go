package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator
var workerSockName string

const (
	maxRetries = 5
)

type WorkerTaskState struct {
	tasks        Tasks
	intermediate []KeyValue
	config       WorkerConfig
}

// start a thread that listens for RPCs from worker.go
func (wts *WorkerTaskState) server(sockname string) {
	rpc.Register(wts)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v\n", sockname, e)
	}
	go http.Serve(l, nil)
}

func (wts *WorkerTaskState) HeartbeatPing(_ Tasks, reply *Tasks) error {
	*reply = wts.tasks
	return nil
}
func (wts *WorkerTaskState) reportTaskFailed(c *rpc.Client, id TaskId, taskType TaskType) {
	var reply struct{}
	callrpc(c, "Coordinator.ReportTaskFailed", TaskReport{
		TaskId: id,
		Type:   taskType,
	}, &reply)
}

// main/mrworker.go calls this function.
// A reasonable naming convention for intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	coordSockName = sockname
	workerSockName = coordinatorSock()
	//reduce
	mapOutDoneCount := 0

	wts := WorkerTaskState{
		tasks: Tasks{
			Current: -1,
		},
		intermediate: make([]KeyValue, 0),
		config: WorkerConfig{
			ReduceTasksCount: 0,
			MapTasksCount:    0,
		}}

	retries := 0

CONNECT_COORDINATOR:
	c, err := connectrpc(coordSockName)
	if err != nil {
		if retries < maxRetries {
			retries++
			time.Sleep(100 * time.Millisecond)
			goto CONNECT_COORDINATOR
		}
		return
	}
	retries = 0

	err = callrpc(c, "Coordinator.GetWorkerConfig", WorkerInfo{
		Sockname: workerSockName,
	}, &wts.config)
	if err != nil {
		goto CONNECT_COORDINATOR
	}
	wts.tasks.Received = NewBitmap((wts.config.MapTasksCount + wts.config.ReduceTasksCount))
	wts.tasks.Status = NewBitmap((wts.config.MapTasksCount + wts.config.ReduceTasksCount))
	wts.tasks.CurrentRPartitionToMapOutDone = NewBitmap((wts.config.MapTasksCount))

	// for heartbeat messages between coordinator and worker
	func(wts *WorkerTaskState) { wts.server(workerSockName) }(&wts)

	defer c.Close()

REQUEST_TASK:
	for {
		var task Task

		err = callrpc(c, "Coordinator.GetTask", WorkerInfo{
			Sockname: workerSockName,
			Tasks:    wts.tasks,
		}, &task)

		if err != nil {
			if retries < maxRetries {
				retries++
				time.Sleep(200 * time.Millisecond)
				goto CONNECT_COORDINATOR
			}
			return
		}
		switch task.Type {
		case MapTask:
			file, err := os.Open(task.FileChunks[0].Path)
			if err != nil {
				wts.reportTaskFailed(c, task.TaskId, task.Type)
				time.Sleep(500 * time.Millisecond)
				goto REQUEST_TASK
			}
			content, err := io.ReadAll(file)
			if err != nil {
				wts.reportTaskFailed(c, task.TaskId, task.Type)
				time.Sleep(500 * time.Millisecond)
				goto REQUEST_TASK
			}
			//set task received
			wts.tasks.Received.Set(uint32(task.TaskId))
			file.Close()
			kva := mapf(task.FileChunks[0].Path, string(content))

			fps := make([]*os.File, 0, wts.config.ReduceTasksCount)
			mapReduceTasks := make([]string, 0, wts.config.ReduceTasksCount)

			for partition := range wts.config.ReduceTasksCount {
				writeTo := fmt.Sprintf("mr-%d-%d", task.TaskId, partition)
				fp, err := os.OpenFile(writeTo, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					//report error, reset task state
					wts.reportTaskFailed(c, task.TaskId, task.Type)
					wts.tasks.Received.Clear(uint32(task.TaskId))
					time.Sleep(500 * time.Millisecond)
					goto REQUEST_TASK
				}
				mapReduceTasks = append(mapReduceTasks, writeTo)
				fps = append(fps, fp)
			}

			//TODO: use a combiner function --use a flag and compare memory usage and performance metrics with or without it
			for _, kv := range kva {
				partition := ihash(kv.Key) % (int)(wts.config.ReduceTasksCount)
				fp := fps[partition]
				enc := json.NewEncoder(fp)
				err = enc.Encode(&kv)
				if err != nil {
					wts.reportTaskFailed(c, task.TaskId, task.Type)
					wts.tasks.Received.Clear(uint32(task.TaskId))
					time.Sleep(500 * time.Millisecond)
					goto REQUEST_TASK
				}
			}
			wts.tasks.Status.Set(uint32(task.TaskId))
			go func() {
				for _, fp := range fps {
					fp.Close()
				}
			}()
		case ReduceTask:
			wts.tasks.Current = int(task.TaskId)
			wts.tasks.Received.Set(uint32(task.TaskId) + wts.config.MapTasksCount)
			intermediate := wts.intermediate
			for _, mapOutput := range task.FileChunks {
				file, err := os.Open(mapOutput.Path)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				file.Close()
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}

				wts.tasks.CurrentRPartitionToMapOutDone.Set(mapOutput.Index)
				mapOutDoneCount++
			}
			if mapOutDoneCount != int(wts.config.MapTasksCount) {
				goto REQUEST_TASK
			}
			mapOutDoneCount = 0
			sort.Sort(ByKey(intermediate))
			// combiner code
			i := 0
			writeTo := fmt.Sprintf("mr-out-%d", task.TaskId)
			//TODO: temp to "mr-out-%d" atomic rename
			ofile, err := os.OpenFile(writeTo, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

			if err != nil {
				wts.reportTaskFailed(c, task.TaskId, task.Type)
				wts.tasks.Received.Clear(uint32(task.TaskId) + wts.config.MapTasksCount)
				wts.tasks.Current = -1
				wts.tasks.CurrentRPartitionToMapOutDone.Reset()
				wts.intermediate = make([]KeyValue, 0)
				time.Sleep(500 * time.Millisecond)
				goto REQUEST_TASK
			}

			for i < len(intermediate) {
				j := i + 1
				//compares the current and subsequent keys and increment j if they are the same
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				// this is the correct format for each line of Reduce output.
				i = j
			}

			ofile.Close()
			wts.tasks.Status.Set(uint32(wts.tasks.Current) + wts.config.MapTasksCount)
			wts.tasks.Current = -1
			wts.tasks.CurrentRPartitionToMapOutDone.Reset()
		case WaitTask:
			time.Sleep(200 * time.Millisecond) // short backoff, not 2s — you're just polling for the next free task
			continue REQUEST_TASK
		case ExitTask:
			return
		}

	}
}

// send an RPC request to the coordinator, wait for the response.
func callrpc(c *rpc.Client, rpcname string, arg any, reply any) error {
	err := c.Call(rpcname, arg, reply)
	if err != nil {
		return err
	}
	return nil
}

func connectrpc(sockName string) (*rpc.Client, error) {
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		return nil, err
	}
	return c, nil
}
