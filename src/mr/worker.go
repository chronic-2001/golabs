package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
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

	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	// CallExample()
	var old *Task = &Task{}
	var new *Task = &Task{}

	for {
		call("Master.GetTask", old, new)
		if new.StartTime.IsZero() {
			old = &Task{}
			time.Sleep(time.Second)
			continue
		}
		if new.Type == MAP {
			filename := new.Files[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			n := len(new.Files)
			files := make([]*os.File, n)
			for i := range files {
				files[i], err = ioutil.TempFile(TEMP_DIR,
					strings.Join([]string{"mr", strconv.Itoa(new.Id), strconv.Itoa(i)}, "-"))
				if err != nil {
					log.Fatalf("cannot create temp file")
				}
				new.Files[i] = files[i].Name()
			}
			entries := mapf(filename, string(content))
			for _, e := range entries {
				h := ihash(e.Key) % n
				fmt.Fprintf(files[h], "%v %v\n", e.Key, e.Value)
			}
			for _, file := range files {
				file.Close()
			}
		} else {
			outName := "mr-out-" + strconv.Itoa(new.Id)
			out, _ := ioutil.TempFile(".", outName)
			entries := []KeyValue{}
			for _, filename := range new.Files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					arr := strings.Split(scanner.Text(), " ")
					entries = append(entries, KeyValue{arr[0], arr[1]})
				}
				file.Close()
			}
			sort.Sort(ByKey(entries))
			i := 0
			for i < len(entries) {
				j := i + 1
				for j < len(entries) && entries[j].Key == entries[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, entries[k].Value)
				}
				output := reducef(entries[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(out, "%v %v\n", entries[i].Key, output)
				i = j
			}
			out.Close()
			os.Rename(out.Name(), outName)
		}
		old = new
		new = &Task{}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
