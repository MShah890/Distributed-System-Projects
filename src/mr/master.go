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

// MutexCondPair is a pair of mutex and condition variable
type MutexCondPair struct {
	mut  sync.Mutex
	cond sync.Cond
}

// Master has master's data members
type Master struct {
	// Your definitions here.
	totalMapTasks        int
	mapTaskNum           int
	mapTaskComplete      int
	reduceTaskComplete   int
	reduceTaskCount      int
	mapTaskStatus        map[string]int
	reduceTaskStatus     map[int]int
	mapReduceCond        map[int]*sync.Cond
	mapFileCond          map[string]*sync.Cond
	mapFileNum           map[string]int
	mapTaskQ             chan string
	reduceTaskQ          chan int
	InterFilesPerRedTask [][]string
	dataMutex            sync.Mutex
}

// Shows the different status of tasks
const (
	NotStarted = iota
	Processing
	Finished
)

// RPC handlers for the worker to call.

// GetTask Returns a filePath to the worker that calls it if a file is yet to be processed
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	select {
	case fileName := <-m.mapTaskQ:
		reply.FilePath = fileName
		reply.MapTaskNum = m.mapFileNum[fileName]
		reply.TaskType = "Map"
		reply.ReduceTaskCount = m.reduceTaskCount
		// fmt.Println(fileName)
		m.mapFileCond[fileName].L.Lock()
		m.mapTaskStatus[fileName] = Processing
		m.mapFileCond[fileName].L.Unlock()
		go m.checkIfWorkerFinishesTask(fileName, -1, "Map")
		return nil
	case reduceTask := <-m.reduceTaskQ:
		reply.ReduceTaskNum = reduceTask
		reply.FilePathList = m.InterFilesPerRedTask[reduceTask]
		reply.TaskType = "Reduce"
		m.mapReduceCond[reduceTask].L.Lock()
		m.reduceTaskStatus[reduceTask] = Processing
		m.mapReduceCond[reduceTask].L.Unlock()
		go m.checkIfWorkerFinishesTask("", reduceTask, "Reduce")
		return nil
	default:
		m.dataMutex.Lock()
		defer m.dataMutex.Unlock()
		if m.reduceTaskCount == m.reduceTaskComplete {
			reply.TaskType = "Clean Exit"
		}
		return nil
	}
}

// CompleteTask ... Worker notifies master of task completion
func (m *Master) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	if args.TaskType == "Map" {
		// fmt.Println("Worker finishes map task " + args.FileSplitName)
		m.mapFileCond[args.FileSplitName].L.Lock()
		if m.mapTaskStatus[args.FileSplitName] != Finished {
			m.mapTaskStatus[args.FileSplitName] = Finished
			m.dataMutex.Lock()
			m.mapTaskComplete++
			m.dataMutex.Unlock()
			m.addInterFiles(args.FilePathList, "Map")
			if m.mapTaskComplete == m.totalMapTasks {
				m.addReduceTasksToQ()
			}
		}

		m.mapFileCond[args.FileSplitName].Signal()
		m.mapFileCond[args.FileSplitName].L.Unlock()
	} else if args.TaskType == "Reduce" {
		// fmt.Println("Worker finishes reduce task " + strconv.Itoa(args.ReduceTaskNum))
		m.mapReduceCond[args.ReduceTaskNum].L.Lock()
		if m.reduceTaskStatus[args.ReduceTaskNum] != Finished {
			m.reduceTaskStatus[args.ReduceTaskNum] = Finished
			m.dataMutex.Lock()
			m.reduceTaskComplete++
			m.dataMutex.Unlock()
			m.addInterFiles(args.FilePathList, "Reduce")
		}

		m.mapReduceCond[args.ReduceTaskNum].Signal()
		m.mapReduceCond[args.ReduceTaskNum].L.Unlock()
		// fmt.Println("Returning from get task")
	}

	return nil
}

func (m *Master) addReduceTasksToQ() {
	for i := 0; i < m.reduceTaskCount; i++ {
		m.reduceTaskQ <- i
		m.reduceTaskStatus[i] = NotStarted
		mtx := sync.Mutex{}
		cond := sync.Cond{L: &mtx}
		m.mapReduceCond[i] = &cond
	}
}

func (m *Master) wakeUpMasterAfter10Sec(fileName string, reduceTaskNum int, taskType string) {

	time.Sleep(10 * time.Second)

	if taskType == "Map" {
		m.mapFileCond[fileName].L.Lock()
		m.mapFileCond[fileName].Signal()
		m.mapFileCond[fileName].L.Unlock()
	} else if taskType == "Reduce" {
		m.mapReduceCond[reduceTaskNum].L.Lock()
		m.mapReduceCond[reduceTaskNum].Signal()
		m.mapReduceCond[reduceTaskNum].L.Unlock()
	}
}

func (m *Master) checkIfWorkerFinishesTask(fileName string, reduceTaskNum int, taskType string) {
	go m.wakeUpMasterAfter10Sec(fileName, reduceTaskNum, taskType)
	if taskType == "Map" {
		m.mapFileCond[fileName].L.Lock()
		if m.mapTaskStatus[fileName] != Finished {
			// fmt.Println("Master waiting for map task " + fileName)
			m.mapFileCond[fileName].Wait()
			if m.mapTaskStatus[fileName] != Finished {
				m.mapTaskQ <- fileName
				m.mapTaskStatus[fileName] = NotStarted
			}
		}
		m.mapFileCond[fileName].L.Unlock()
	} else if taskType == "Reduce" {
		m.mapReduceCond[reduceTaskNum].L.Lock()
		if m.reduceTaskStatus[reduceTaskNum] != Finished {
			// fmt.Println("Master waiting for reduce task " + strconv.Itoa(reduceTaskNum))
			m.mapReduceCond[reduceTaskNum].Wait()
			// fmt.Println("Master thread waiting for reduce task " + strconv.Itoa(reduceTaskNum) + "awoken")
			if m.reduceTaskStatus[reduceTaskNum] != Finished {
				m.reduceTaskQ <- reduceTaskNum
				m.reduceTaskStatus[reduceTaskNum] = NotStarted
			}
		}
		m.mapReduceCond[reduceTaskNum].L.Unlock()
	}
}

func (m *Master) addInterFiles(interFileList []string, taskType string) {
	if taskType == "Map" {
		for reduceTaskNum, fileName := range interFileList {
			newFile := fileName[5:]
			os.Rename(fileName, newFile)
			m.InterFilesPerRedTask[reduceTaskNum] = append(m.InterFilesPerRedTask[reduceTaskNum], newFile)
		}
	} else if taskType == "Reduce" {
		os.Rename(interFileList[0], interFileList[0][5:])
		// fmt.Println("returning from renaming reduce file")
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
	m.dataMutex.Lock()
	defer m.dataMutex.Unlock()
	return m.reduceTaskComplete == m.reduceTaskCount
}

// MakeMaster creates a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.mapTaskQ = make(chan string, len(files))
	m.reduceTaskQ = make(chan int, nReduce)
	m.InterFilesPerRedTask = make([][]string, nReduce)
	m.mapTaskStatus = make(map[string]int)
	m.reduceTaskStatus = make(map[int]int)
	m.mapFileCond = make(map[string]*sync.Cond)
	m.mapReduceCond = make(map[int]*sync.Cond)
	m.reduceTaskCount = nReduce
	m.totalMapTasks = len(files)
	m.mapFileNum = make(map[string]int)
	count := 1
	for _, fileName := range files {
		m.mapTaskStatus[fileName] = NotStarted
		mtx := sync.Mutex{}
		cond := sync.Cond{L: &mtx}
		m.mapFileCond[fileName] = &cond
		m.mapTaskQ <- fileName
		m.mapFileNum[fileName] = count
		count++
	}
	m.server()
	return &m
}
