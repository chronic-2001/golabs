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

const TEMP_DIR = "temp"

const (
	MAP    = "map"
	REDUCE = "reduce"
)
const (
	IDLE = iota
	IN_PROGRESS
	COMPLETED
)

type Master struct {
	// Your definitions here.
	lock        sync.Mutex
	mapTasks    []*Task
	reduceTasks []*Task
}

type Task struct {
	Type      string
	Id        int
	State     int
	StartTime time.Time
	Files     []string
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(old *Task, new *Task) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !old.StartTime.IsZero() {
		if old.Type == MAP {
			m.mapTasks[old.Id].State = COMPLETED
			for i, f := range old.Files {
				m.reduceTasks[i].Files[old.Id] = f
			}
		} else {
			m.reduceTasks[old.Id].State = COMPLETED
		}
	}

	mapCompleted := true
	for _, t := range m.mapTasks {
		if t.State == IDLE {
			t.State = IN_PROGRESS
			t.StartTime = time.Now()
			*new = *t
			mapCompleted = false
			break
		} else if t.State == IN_PROGRESS {
			mapCompleted = false
		}
	}
	if mapCompleted {
		for _, t := range m.reduceTasks {
			if t.State == IDLE {
				t.State = IN_PROGRESS
				t.StartTime = time.Now()
				*new = *t
				break
			}
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	ret := true

	// Your code here.

	m.lock.Lock()
	for _, t := range m.mapTasks {
		ret = ret && checkTask(t)
	}

	for _, t := range m.reduceTasks {
		ret = ret && checkTask(t)
	}
	m.lock.Unlock()

	if ret {
		os.RemoveAll(TEMP_DIR)
	}

	return ret
}

func checkTask(task *Task) bool {
	if task.State == IN_PROGRESS && time.Since(task.StartTime) > 10*time.Second {
		task.State = IDLE
		task.StartTime = time.Time{}
	}
	return task.State == COMPLETED
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mapTasks = make([]*Task, len(files))
	for i, f := range files {
		task := &Task{Type: MAP, Id: i, State: IDLE, Files: make([]string, nReduce)}
		task.Files[0] = f
		m.mapTasks[i] = task
	}
	m.reduceTasks = make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = &Task{Type: REDUCE, Id: i, State: IDLE, Files: make([]string, len(files))}
	}

	os.Mkdir(TEMP_DIR, 0700)

	m.server()
	return &m
}
