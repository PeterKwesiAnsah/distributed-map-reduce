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
var waitTilDone chan bool

type WorkerTaskState struct {
	task Task
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

func (wts *WorkerTaskState) TaskStatusCheck(taskId TaskId, reply *TaskStatus) error {
	if taskId != wts.task.TaskId {
		return fmt.Errorf("Expected %d taskId but got %d. Check state on master.\n", wts.task.TaskId, taskId)
	}
	if wts.task.Status {
		waitTilDone <- true
	}
	return nil
}

// main/mrworker.go calls this function.
// A reasonable naming convention for intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	waitTilDone = make(chan bool)
	coordSockName = sockname
	workerSockName = coordinatorSock()
	runningPhase := MapTask
	wts := WorkerTaskState{}
	// for heartbeat messages between coordinator and worker
	func() { wts.server(workerSockName) }()
	for {
		fmt.Printf("Starting worker with PID %d\n", os.Getpid())
		wts.task.Status = false
		c, err := connectrpc(coordSockName)
		if err != nil {
			return
		}

		err = callrpc(c, "Coordinator.GetTask", WorkerInfo{
			WorkerId: WorkerId(os.Getpid()),
			Type:     0,
			Sockname: workerSockName,
		}, &wts.task)
		defer c.Close()
		if err != nil {
			fmt.Printf(fmt.Errorf("Worker Id %d: %v\n", wts.task.TaskId, err).Error())
			continue
		}
		runningPhase = wts.task.Type
		if runningPhase == MapTask {
			fmt.Printf("Starting worker in map phase with PID %d\n", os.Getpid())
			file, err := os.Open(wts.task.Path)
			if err != nil {
				log.Fatalf("cannot open %v\n", wts.task.Path)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v\n", wts.task.Path)
			}
			file.Close()
			kva := mapf(wts.task.Path, string(content))

			fps := make([]*os.File, 0, wts.task.ReduceTasks)
			mapReduceTasks := make([]string, 0, wts.task.ReduceTasks)

			for partition := range wts.task.ReduceTasks {
				writeTo := fmt.Sprintf("mr-%d-%d", wts.task.TaskId, partition)
				fp, err := os.OpenFile(writeTo, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					log.Fatalf("cannot open %v\n", writeTo)
				}
				mapReduceTasks = append(mapReduceTasks, writeTo)
				fps = append(fps, fp)
			}

			//TODO: use a combiner function --use a flag and compare memory usage and performance metrics with or without it

			for _, kv := range kva {
				partition := ihash(kv.Key) % (int)(wts.task.ReduceTasks)
				writeTo := mapReduceTasks[partition]
				fp := fps[partition]
				enc := json.NewEncoder(fp)
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write to buffered Key/Value Pair to %v\n", writeTo)
				}
			}
			var reply NotifyStatus
			err = callrpc(c, "Coordinator.OutputReducePartitions", NotifyTaskOutput{
				ReduceTasks: mapReduceTasks,
			}, &reply)
			if err != nil {
				fmt.Printf(fmt.Errorf("Worker Id %d: %v\n", wts.task.TaskId, err).Error())
				continue
			}
			go func() {
				for _, fp := range fps {
					fp.Close()
				}
			}()
			wts.task.Status = true
			<-waitTilDone

		} else {
			intermediate := []KeyValue{}
			for _, reducerTask := range wts.task.MapOutputs {
				file, err := os.Open(reducerTask)
				if err != nil {
					log.Fatal(err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			// combiner code
			i := 0
			writeTo := fmt.Sprintf("mr-out-%d", wts.task.TaskId)
			ofile, err := os.OpenFile(writeTo, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

			if err != nil {
				log.Fatal("Failed to open file\n")
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
				// rs.counts.Keys++
				// rs.counts.Values += len(values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				// this is the correct format for each line of Reduce output.
				i = j
			}
			ofile.Close()
			wts.task.Status = true
			<-waitTilDone
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
func callrpc(c *rpc.Client, rpcname string, arg any, reply any) error {
	err := c.Call(rpcname, arg, reply)
	//blocking
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
