package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

type ShardStatus int32

const (
	Ready ShardStatus = iota
	Receiving
	Sending
)

type Shard struct {
	Index      int
	Status     ShardStatus
	Values     map[string]string
	RequestIds map[int64]int64
}

type MigrateArgs struct {
	ConfigNum int
	Shards    []Shard
	ClientId  int64
	RequestId int64
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
	Command   command
}

type command interface {
	execute(kv *ShardKV) Reply
}

type Config shardmaster.Config

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	master       *shardmaster.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	shards    [shardmaster.NShards]Shard
	channels  map[int]chan Reply
	config    Config
	clientId  int64
	requestId int64
}

func (kv *ShardKV) Get(args *GetArgs, reply *Reply) {
	// Your code here.
	*reply = kv.waitForApply(&Op{args.ClientId, args.RequestId, args})
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *Reply) {
	// Your code here.
	*reply = kv.waitForApply(&Op{args.ClientId, args.RequestId, args})
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *Reply) {
	// Your code here.
	*reply = kv.waitForApply(&Op{args.ClientId, args.RequestId, args})
}

func (args *GetArgs) execute(kv *ShardKV) Reply {
	reply := Reply{}
	shard := &kv.shards[key2shard(args.Key)]
	reply.Err = kv.checkShard(shard)
	if reply.Err == OK {
		reply.Value = shard.Values[args.Key]
	}
	return reply
}

func (args *PutAppendArgs) execute(kv *ShardKV) Reply {
	reply := Reply{}
	shard := &kv.shards[key2shard(args.Key)]
	reply.Err = kv.checkShard(shard)
	if reply.Err == OK {
		if args.RequestId > shard.RequestIds[args.ClientId] {
			if args.Op == "Put" {
				shard.Values[args.Key] = args.Value
			} else {
				shard.Values[args.Key] += args.Value
			}
			shard.RequestIds[args.ClientId] = args.RequestId
		}
	}
	return reply
}

func (args *MigrateArgs) execute(kv *ShardKV) Reply {
	reply := Reply{Err: OK}
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrWrongLeader
	} else if args.ConfigNum == kv.config.Num {
		for _, shard := range args.Shards {
			myShard := &kv.shards[shard.Index]
			if myShard.Status != Ready {
				myShard.Status = Ready
				myShard.Values = clone(shard.Values)
				myShard.RequestIds = clone(shard.RequestIds)
			}
		}
	}
	return reply
}

func (kv *ShardKV) checkShard(shard *Shard) Err {
	if shard.Status == Receiving {
		return ErrWrongLeader
	} else if shard.Status == Sending || shard.Values == nil {
		return ErrWrongGroup
	} else {
		return OK
	}
}

func (config Config) execute(kv *ShardKV) Reply {
	if config.Num == kv.config.Num+1 {
		for i, gid := range config.Shards {
			oldGid := kv.config.Shards[i]
			if gid != kv.gid && oldGid == kv.gid {
				kv.shards[i].Status = Sending
			} else if gid == kv.gid && oldGid != kv.gid {
				if oldGid == 0 {
					kv.shards[i].Values = make(map[string]string)
					kv.shards[i].RequestIds = make(map[int64]int64)
				} else {
					kv.shards[i].Status = Receiving
				}
			}
		}
		kv.config = config
	}

	return Reply{Err: OK}
}

func (kv *ShardKV) waitForApply(op *Op) Reply {
	reply := Reply{Err: ErrWrongLeader}
	if index, _, isLeader := kv.rf.Start(op); isLeader {
		kv.mu.Lock()
		ch := make(chan Reply, 1)
		kv.channels[index] = ch
		kv.mu.Unlock()
		select {
		case reply = <-ch:
			if reply.ClientId != op.ClientId || reply.RequestId != op.RequestId {
				reply.Err = ErrWrongLeader
			}
		case <-time.After(500 * time.Millisecond):
		}
		kv.mu.Lock()
		delete(kv.channels, index)
		kv.mu.Unlock()
	}
	return reply
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if len(data) > 0 {
		buffer := bytes.NewBuffer(data)
		d := labgob.NewDecoder(buffer)
		d.Decode(&kv.config)
		d.Decode(&kv.shards)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func clone[K comparable, V any](source map[K]V) map[K]V {
	if source == nil {
		return nil
	}
	target := make(map[K]V)
	for k, v := range source {
		target[k] = v
	}
	return target
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})
	labgob.Register(&GetArgs{})
	labgob.Register(&PutAppendArgs{})
	labgob.Register(&MigrateArgs{})
	labgob.Register(Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.master = shardmaster.MakeClerk(masters)

	// Your initialization code here.
	kv.channels = make(map[int]chan Reply)
	kv.clientId = nrand()
	for i := range kv.shards {
		kv.shards[i].Index = i
	}
	kv.readSnapshot(persister.ReadSnapshot())

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.Maxraftstate = maxraftstate

	go func() {
		for applyMsg := range kv.applyCh {
			kv.mu.Lock()
			if applyMsg.CommandValid {
				op := applyMsg.Command.(*Op)
				reply := op.Command.execute(kv)
				if ch, ok := kv.channels[applyMsg.CommandIndex]; ok {
					reply.ClientId = op.ClientId
					reply.RequestId = op.RequestId
					ch <- reply
				}
			} else {
				if applyMsg.Snapshot == nil {
					buffer := new(bytes.Buffer)
					e := labgob.NewEncoder(buffer)
					e.Encode(kv.config)
					e.Encode(kv.shards)
					kv.applyCh <- raft.ApplyMsg{Snapshot: buffer.Bytes()}
				} else {
					kv.readSnapshot(applyMsg.Snapshot)
				}
			}
			kv.mu.Unlock()
		}
	}()

	go kv.setInterval(kv.pollConfig, 100*time.Millisecond)
	go kv.setInterval(kv.sendShards, 100*time.Millisecond)
	return kv
}

func (kv *ShardKV) setInterval(action func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

func (kv *ShardKV) pollConfig() {
	kv.mu.Lock()
	reconfiguring := false
	for _, shard := range kv.shards {
		if shard.Status != Ready {
			reconfiguring = true
			break
		}
	}
	configNum := kv.config.Num
	kv.mu.Unlock()
	if reconfiguring {
		return
	}
	nextConfig := Config(kv.master.Query(configNum + 1))
	if nextConfig.Num == configNum+1 {
		kv.waitForApply(&Op{kv.clientId, atomic.AddInt64(&kv.requestId, 1), nextConfig})
	}
}

func (kv *ShardKV) sendShards() {
	kv.mu.Lock()
	sendingShards := make(map[int][]Shard)
	for _, shard := range kv.shards {
		if shard.Status == Sending && shard.Values != nil {
			gid := kv.config.Shards[shard.Index]
			shard.Values = clone(shard.Values)
			shard.RequestIds = clone(shard.RequestIds)
			sendingShards[gid] = append(sendingShards[gid], shard)
		}
	}
	var wg sync.WaitGroup
	if len(sendingShards) > 0 {
		for gid, shards := range sendingShards {
			args := MigrateArgs{
				kv.config.Num,
				shards,
				kv.clientId,
				atomic.AddInt64(&kv.requestId, 1)}
			servers := kv.config.Groups[gid]
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, server := range servers {
					srv := kv.make_end(server)
					var reply Reply
					ok := srv.Call("ShardKV.Migrate", &args, &reply)
					if ok && reply.Err == OK {
						for i := range args.Shards {
							args.Shards[i].Values = nil
							args.Shards[i].RequestIds = nil
						}
						args.RequestId = atomic.AddInt64(&kv.requestId, 1)
						kv.waitForApply(&Op{args.ClientId, args.RequestId, &args})
						return
					}
				}
			}()
		}
	}
	kv.mu.Unlock()
	wg.Wait()

}
