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

type workerState int
type channelType int

const (
	in_progress workerState = iota
	completed
	failed
)

const (
	scheduler channelType = iota
	monitor
	cdone
)

type taskStatus struct {
	//workerId WorkerId
	state workerState
	info  WorkerInfo
}

// TODO: Read/Write locks for mTasks and rTasks
type Coordinator struct {
	mTasks       *[][]taskStatus
	mu           sync.Mutex
	runningPhase TaskType
	mOutput      *[][]string
	rTasks       *[][]taskStatus
	mu_moutput   sync.Mutex
}

type workerChannelItem struct {
	info   WorkerInfo
	taskId TaskId
}
type workerTasks struct {
	completed chan workerChannelItem
	pending   chan workerChannelItem
	failed    chan workerChannelItem
}

var reduceTasksCnt int
var mapTasksCnt int
var completedRTasks = 0

var quitScheduler chan bool

type channelTasks [3]workerTasks

var workerChannels channelTasks

// idle/non-active/nil -> in_progress
func (c *Coordinator) GetTask(arg WorkerInfo, reply *Task) error {
	schedulerChannel := workerChannels[scheduler]
	defer c.mu.Unlock()
	c.mu.Lock()
	if c.runningPhase == MapTask {
		mapTasksFiles := os.Args[2:]
		for mapTaskId, file := range mapTasksFiles {
			mTask := (*c.mTasks)[mapTaskId]
			if mTask == nil {
				(*c.mTasks)[mapTaskId] = []taskStatus{
					{info: arg, state: in_progress},
				}
				*reply = Task{
					Path:        file,
					ReduceTasks: (uint32)(reduceTasksCnt),
					TaskId:      (TaskId)(mapTaskId),
					Type:        MapTask,
					Status:      false,
				}
				schedulerChannel.pending <- workerChannelItem{info: arg, taskId: (TaskId)(mapTaskId)}
				return nil
			} else {
				lastStatus := mTask[len(mTask)-1]
				if lastStatus.state == failed {
					c.markTask(0, (TaskId)(mapTaskId), arg, in_progress)
					*reply = Task{
						Path:        file,
						ReduceTasks: (uint32)(reduceTasksCnt),
						TaskId:      (TaskId)(mapTaskId),
						Type:        MapTask,
						Status:      false,
					}
					schedulerChannel.pending <- workerChannelItem{info: arg, taskId: (TaskId)(mapTaskId)}
					return nil
				}
			}
		}
		goto HANDLE_ERROR
	}

	for reduceTaskId := range reduceTasksCnt {
		rTask := (*c.rTasks)[reduceTaskId]
		if rTask == nil {
			*reply = Task{
				MapOutputs: (*c.mOutput)[reduceTaskId],
				TaskId:     (TaskId)(reduceTaskId),
				Type:       ReduceTask,
				Status:     false,
			}
			(*c.rTasks)[reduceTaskId] = []taskStatus{{info: arg, state: in_progress}}
			schedulerChannel.pending <- workerChannelItem{info: arg, taskId: (TaskId)(reduceTaskId)}
			return nil
		} else {
			if rTask[len(rTask)-1].state == failed {
				c.markTask(1, (TaskId)(reduceTaskId), arg, in_progress)
				*reply = Task{
					MapOutputs: (*c.mOutput)[reduceTaskId],
					TaskId:     (TaskId)(reduceTaskId),
					Type:       ReduceTask,
					Status:     false,
				}
				schedulerChannel.pending <- workerChannelItem{info: arg, taskId: (TaskId)(reduceTaskId)}
				return nil
			}
		}
		goto HANDLE_ERROR
	}
HANDLE_ERROR:
	return fmt.Errorf("All tasks are inprogress or completed\n")
}

// in_progress -> completed
func (c *Coordinator) OutputReducePartitions(arg NotifyTaskOutput, reply *NotifyStatus) error {
	if reduceTasksCnt != len(arg.ReduceTasks) {
		*reply = -1 //INSUF_RT
		return fmt.Errorf("Expected %d but got %d\n", reduceTasksCnt, len(arg.ReduceTasks))
	}
	*reply = 0
	defer c.mu_moutput.Unlock()
	c.mu_moutput.Lock()
	for index, reduceTask := range arg.ReduceTasks {
		reduceTasks := (*c.mOutput)[index]
		if reduceTasks == nil {
			(*c.mOutput)[index] = make([]string, 0, reduceTasksCnt)
			reduceTasks = (*c.mOutput)[index]
		}
		reduceTasks = append(reduceTasks, reduceTask)
		(*c.mOutput)[index] = reduceTasks
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ch := workerChannels[cdone].completed
	if completedRTasks == reduceTasksCnt {
		quitScheduler <- true
		return true
	}
	<-ch
	completedRTasks++
	return false
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
	go http.Serve(l, nil)
}

// TODO: 3.6 (Backup Tasks)
// TODO: 4.2 (Ordering Guarantees)
// TODO: 4.3 (Combiner Function)
// TODO: 4.8 (Status Information)
// TODO: 4.9 (Counters)
// TODO: implement task locking, remove global lock

func (wt *channelTasks) initializePending(ch channelType) {
	wt[ch].pending = make(chan workerChannelItem, reduceTasksCnt+mapTasksCnt)
}
func (wt *channelTasks) initializeCompleted(ch channelType) {
	wt[ch].completed = make(chan workerChannelItem, reduceTasksCnt+mapTasksCnt)
}
func (wt *channelTasks) initializeFailed(ch channelType) {
	wt[ch].failed = make(chan workerChannelItem, reduceTasksCnt+mapTasksCnt)
}

func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {

	M := len(files)
	reduceTasksCnt = nReduce
	mapTasksCnt = M

	workerChannels.initializeCompleted(cdone)
	quitScheduler = make(chan bool)

	var mapTasks [][]taskStatus = make([][]taskStatus, M)
	var mapOutput [][]string = make([][]string, nReduce)
	var reduceTasks [][]taskStatus = make([][]taskStatus, nReduce)
	c := Coordinator{mTasks: &mapTasks, rTasks: &reduceTasks, mOutput: &mapOutput, runningPhase: MapTask}
	c.server(sockname)

	go func(mSize int, rSize int) {

		workerChannels.initializeFailed(scheduler)
		workerChannels.initializePending(scheduler)
		workerChannels.initializeCompleted(scheduler)
		compTasks := 0
		inprogressTasks := 0
		failedTasks := 0
		totalTasks := mSize
		phase := c.runningPhase
		for {
			if compTasks == totalTasks && phase == MapTask {
				//send out reduce tasks
				phase = ReduceTask
				totalTasks = rSize
				compTasks = 0
				inprogressTasks = 0
				failedTasks = 0
				c.mu.Lock()
				c.runningPhase = ReduceTask
				c.mu.Unlock()
			}
			select {
			//receieve task and forward it to interested goroutines based on certain conditions
			case t := <-workerChannels[scheduler].pending:
				inprogressTasks++
				workerChannels[monitor].pending <- t
			case t := <-workerChannels[scheduler].completed:
				compTasks++
				inprogressTasks--
				if phase == ReduceTask {
					workerChannels[cdone].completed <- t
				}
				c.mu.Lock()
				c.markTask(t.info.Type, t.taskId, t.info, completed)
				c.mu.Unlock()
			case t := <-workerChannels[scheduler].failed:
				failedTasks++
				inprogressTasks--
				c.mu.Lock()
				c.markTask(t.info.Type, t.taskId, t.info, failed)
				c.mu.Unlock()
			case <-quitScheduler:
				//clean up
				return
			}
		}
	}(M, nReduce)

	go c.workerMonitor()

	return &c
}

func (c *Coordinator) markTask(typ TaskType, taskId TaskId, info WorkerInfo, state workerState) {
	var tasks *[][]taskStatus
	if typ == MapTask {
		tasks = c.mTasks
	} else {
		tasks = c.rTasks
	}

	task := (*tasks)[taskId]

	task = append(task, taskStatus{info: info, state: state})
	(*tasks)[taskId] = task
}

func (c *Coordinator) workerMonitor() {
	workerChannels.initializePending(monitor)
	for {
		worker := <-workerChannels[monitor].pending
		client, err := connectrpc(worker.info.Sockname)

		if err != nil {
			workerChannels[scheduler].failed <- worker
			continue
		}
		go func(client *rpc.Client, worker workerChannelItem) {
			done := make(chan *rpc.Call, 1)
			for {
				timeout := time.After(1 * time.Second)
				var taskStatus *TaskStatus
				client.Go("Worker.TaskStatusCheck", worker.taskId, taskStatus, done)
				select {
				case <-timeout:
					workerChannels[scheduler].failed <- worker
					return
				case c := <-done:
					if c.Error != nil {
						workerChannels[scheduler].failed <- worker
						return
					}
					taskStatus = c.Reply.(*TaskStatus)
					if *taskStatus {
						workerChannels[scheduler].completed <- worker
						return
					}
				}

			}
		}(client, worker)
	}
}
