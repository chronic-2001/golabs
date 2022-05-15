package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
	Name      string
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	values     map[string]string
	requestIds map[int64]int64
	channels   map[string]chan string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Value, reply.Err = kv.waitForApply(&Op{args.ClientId, args.RequestId, "Get", args.Key, ""})
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, reply.Err = kv.waitForApply(&Op{args.ClientId, args.RequestId, args.Op, args.Key, args.Value})
}

func (kv *KVServer) waitForApply(op *Op) (string, Err) {
	var value string
	var err Err = ErrWrongLeader
	if _, _, isLeader := kv.rf.Start(op); isLeader {
		kv.mu.Lock()
		ch := make(chan string, 1)
		chKey := fmt.Sprintf("%d_%d", op.ClientId, op.RequestId)
		kv.channels[chKey] = ch
		kv.mu.Unlock()
		select {
		case value = <-ch:
			err = ""
		case <-time.After(500 * time.Millisecond):
		}
		kv.mu.Lock()
		delete(kv.channels, chKey)
		kv.mu.Unlock()
	}

	return value, err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.values = make(map[string]string)
	kv.requestIds = make(map[int64]int64)
	kv.channels = make(map[string]chan string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for applyMsg := range kv.applyCh {
			kv.mu.Lock()
			if applyMsg.CommandValid {
				op := applyMsg.Command.(*Op)
				if op.RequestId > kv.requestIds[op.ClientId] {
					switch op.Name {
					case "Put":
						kv.values[op.Key] = op.Value
					case "Append":
						kv.values[op.Key] += op.Value
					}
					kv.requestIds[op.ClientId] = op.RequestId
				}
				if ch, ok := kv.channels[fmt.Sprintf("%d_%d", op.ClientId, op.RequestId)]; ok {
					ch <- kv.values[op.Key]
				}
			}
			kv.mu.Unlock()
		}
	}()

	return kv
}
