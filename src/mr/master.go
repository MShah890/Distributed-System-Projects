package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	inputFileSet    list.List
	workerIdFileMap map[int]string
	mapMutex        sync.Mutex
}

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
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()
	if m.inputFileSet.Len() > 0 {
		filePath := m.inputFileSet.Front().Value.(string)
		m.inputFileSet.Remove(m.inputFileSet.Front())
		m.workerIdFileMap[args.workerID] = filePath
		reply.taskAvailable = true
		reply.filePath = filePath
	}
	reply.taskAvailable = false
	reply.filePath = ""
	return nil
}

func ()

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

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	for _, fileName := range files {
		m.inputFileSet.PushBack(fileName)
	}

	m.server()
	return &m
}
