package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// ihash(key) % NReduce is used to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	mapTaskCount, reduceTaskCount := 0, 0
	for true {
		args, reply := GetTaskArgs{}, GetTaskReply{}
		call("Master.GetTask", &args, &reply)

		if reply.TaskType == "Map" {
			mapTaskCount++
			doMap(reply.FilePath, mapf, reply.MapTaskNum, reply.ReduceTaskCount)
		} else if reply.TaskType == "Reduce" {
			reduceTaskCount++
			doReduce(reply.ReduceTaskNum, reducef, reply.FilePathList)
		} else if reply.TaskType == "Clean Exit" {
			os.Exit(0)
		}
	}

}

func doReduce(reduceTaskNum int, reducef func(string, []string) string, filePathList []string) {
	// intermediate := []KeyValue{}
	intermediate := make(map[string][]string)
	type void struct{}
	var member void
	keySet := make(map[string]void) // New empty set

	keys := []string{}
	for _, v := range filePathList {
		// fmt.Println(v)
		file, err := os.Open(v)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", v)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
			_, exists := keySet[kv.Key]
			if !exists {
				keySet[kv.Key] = member
				keys = append(keys, kv.Key)
			}
		}
	}
	sort.Strings(keys)
	outName := "temp-mr-out-" + strconv.Itoa(reduceTaskNum)
	outFile, _ := os.Create(outName)
	for _, key := range keys {
		output := reducef(key, intermediate[key])
		fmt.Fprintf(outFile, "%v %v\n", key, output)
	}

	cargs, creply := CompleteTaskArgs{}, CompleteTaskReply{}
	cargs.TaskType = "Reduce"
	cargs.FilePathList = append(cargs.FilePathList, outName)
	cargs.ReduceTaskNum = reduceTaskNum
	call("Master.CompleteTask", &cargs, &creply)
}

func doMap(filePath string, mapf func(string, string) []KeyValue, mapTaskNum int, reduceTaskCount int) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
	}
	file.Close()
	kva := mapf(filePath, string(content))
	// fmt.Println("Returned from map")
	kvalListPerRed := doPartition(kva, reduceTaskCount)
	// fmt.Println("Returned From Partition")
	fileNames := make([]string, reduceTaskCount)
	for i := 0; i < reduceTaskCount; i++ {
		fileNames[i] = WriteToJSONFile(kvalListPerRed[i], mapTaskNum, i)
	}
	// fmt.Println("Returned From WritingJSON")

	cargs, creply := CompleteTaskArgs{}, CompleteTaskReply{}
	cargs.TaskType = "Map"
	cargs.FilePathList = fileNames
	cargs.FileSplitName = filePath
	call("Master.CompleteTask", &cargs, &creply)
}

func doPartition(kva []KeyValue, reduceTaskCount int) [][]KeyValue {
	kvalListPerRed := make([][]KeyValue, reduceTaskCount)
	for _, kv := range kva {
		v := ihash(kv.Key) % reduceTaskCount
		kvalListPerRed[v] = append(kvalListPerRed[v], kv)
	}
	return kvalListPerRed
}

// WriteToJSONFile writes key value pairs to a json file for a particular reduce task
func WriteToJSONFile(intermediate []KeyValue, mapTaskNum, reduceTaskNUm int) string {
	// fmt.Println("Writing file for task " + strconv.Itoa(mapTaskNum))
	filename := "temp-mr-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(reduceTaskNUm)
	jfile, _ := os.Create(filename)

	enc := json.NewEncoder(jfile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("error: ", err)
		}
	}
	return filename
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
