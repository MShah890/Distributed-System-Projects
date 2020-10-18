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

// MutexCondPair is a pair of mutex and condition variable
type MutexCondPair struct {
	mut  sync.Mutex
	cond sync.Cond
}

// Master has master's data members
type Master struct {
	// Your definitions here.
	mapTaskCount         int
	reduceTaskCount      int
	mapTaskStatus        map[string]int
	mapFileCond          map[string]*sync.Cond
	mapTaskQ             chan string
	reduceTaskQ          chan string
	dataMutex            sync.Mutex
	InterFilesPerRedTask [][]string
}

// Shows the different status of tasks
const (
	NotStarted = iota
	Processing
	Finished
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetTask Returns a filePath to the worker that calls it if a file is yet to be processed
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	select {
	case fileName := <-m.mapTaskQ:
		reply.FilePath = fileName
		reply.MapTaskNum = m.mapTaskCount
		m.dataMutex.Lock()
		fmt.Println(m.mapTaskCount)
		m.mapTaskCount++

		m.dataMutex.Unlock()
		reply.ReduceTaskNum = -1
		reply.TaskType = "Map"
		reply.ReduceTaskCount = m.reduceTaskCount
		m.mapFileCond[fileName].L.Lock()
		m.mapTaskStatus[fileName] = Processing
		go m.checkIfWorkerFinishesTask(fileName)
		m.mapFileCond[fileName].L.Unlock()
		return nil
	}

	// return nil
}

// CompleteTask ... Worker notifies master of task completion
func (m *Master) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	// fmt.Println("2 Worker finishes task for + " + args.FileSplitName)

	if args.TaskType == "Map" {
		fmt.Println("Worker finishes task for + " + args.FileSplitName)
		m.mapFileCond[args.FileSplitName].L.Lock()
		if m.mapTaskStatus[args.FileSplitName] != Finished {
			m.mapTaskStatus[args.FileSplitName] = Finished
			m.addInterFiles(args.FilePathList)
		}

		m.mapFileCond[args.FileSplitName].Signal()
		m.mapFileCond[args.FileSplitName].L.Unlock()
	}

	return nil
}

func (m *Master) wakeUpMasterAfter10Sec(fileName string) {
	time.Sleep(5 * time.Second)
	m.mapFileCond[fileName].L.Lock()
	m.mapFileCond[fileName].Signal()
	m.mapFileCond[fileName].L.Unlock()
}

func (m *Master) checkIfWorkerFinishesTask(fileName string) {
	go m.wakeUpMasterAfter10Sec(fileName)
	m.mapFileCond[fileName].L.Lock()
	if m.mapTaskStatus[fileName] != Finished {
		fmt.Println("Master waiting for task " + fileName)
		m.mapFileCond[fileName].Wait()
		if m.mapTaskStatus[fileName] != Finished {
			m.mapTaskQ <- fileName
			m.mapTaskStatus[fileName] = NotStarted
		}
	}
	m.mapFileCond[fileName].L.Unlock()
}

func (m *Master) addInterFiles(interFileList []string) {
	for reduceTaskNum, fileName := range interFileList {
		newFile := fileName[5:]
		os.Rename(fileName, newFile)
		m.InterFilesPerRedTask[reduceTaskNum] = append(m.InterFilesPerRedTask[reduceTaskNum], newFile)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeMaster creates a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mapTaskQ = make(chan string, len(files))
	m.reduceTaskQ = make(chan string, nReduce)
	m.InterFilesPerRedTask = make([][]string, nReduce)
	m.mapTaskStatus = make(map[string]int)
	m.mapFileCond = make(map[string]*sync.Cond)
	m.mapTaskCount = 0
	m.reduceTaskCount = nReduce
	for _, fileName := range files {
		m.mapTaskStatus[fileName] = NotStarted
		// mcp := &MutexCondPair{}
		mtx := sync.Mutex{}
		cond := sync.Cond{L: &mtx}
		m.mapFileCond[fileName] = &cond
		m.mapTaskQ <- fileName
	}
	m.server()
	return &m
}
