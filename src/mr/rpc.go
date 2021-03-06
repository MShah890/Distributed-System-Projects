package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// GetTaskArgs argument struct
type GetTaskArgs struct {
}

// GetTaskReply reply struct
type GetTaskReply struct {
	FilePath        string
	MapTaskNum      int
	ReduceTaskNum   int
	ReduceTaskCount int
	TaskType        string
	FilePathList    []string
}

// CompleteTaskArgs argument struct
type CompleteTaskArgs struct {
	FilePathList  []string
	FileSplitName string
	ReduceTaskNum int
	TaskType      string
}

// CompleteTaskReply reply struct
type CompleteTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
