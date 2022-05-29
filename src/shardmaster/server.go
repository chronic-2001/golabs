package shardmaster

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs    []Config // indexed by config num
	requestIds map[int64]int64
	channels   map[string]chan struct{}
}

type Op struct {
	// Your data here.
	ClientId  int64
	RequestId int64
	Command   command
}
type command interface {
	execute(sm *ShardMaster)
}

func (sm *ShardMaster) waitForApply(op Op) bool {
	wrongLeader := true
	if _, _, isLeader := sm.rf.Start(op); isLeader {
		sm.mu.Lock()
		ch := make(chan struct{}, 1)
		chKey := fmt.Sprintf("%d_%d", op.ClientId, op.RequestId)
		sm.channels[chKey] = ch
		sm.mu.Unlock()
		select {
		case <-ch:
			wrongLeader = false
		case <-time.After(500 * time.Millisecond):
		}
		sm.mu.Lock()
		delete(sm.channels, chKey)
		sm.mu.Unlock()
	}

	return wrongLeader
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader = sm.waitForApply(Op{args.ClientId, args.RequestId, args})
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader = sm.waitForApply(Op{args.ClientId, args.RequestId, args})
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader = sm.waitForApply(Op{args.ClientId, args.RequestId, args})
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.WrongLeader = sm.waitForApply(Op{args.ClientId, args.RequestId, args})
	if !reply.WrongLeader {
		num := args.Num
		if num == -1 || num >= len(sm.configs) {
			num = len(sm.configs) - 1
		}
		reply.Config = sm.configs[num]
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (args *JoinArgs) execute(sm *ShardMaster) {
	config := sm.newConfig()
	for gid, servers := range args.Servers {
		config.Groups[gid] = servers
	}
	config.rebalance()
}

func (args *LeaveArgs) execute(sm *ShardMaster) {
	config := sm.newConfig()
	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
		for i, g := range config.Shards {
			if g == gid {
				config.Shards[i] = 0
			}
		}
	}
	config.rebalance()
}

func (args *MoveArgs) execute(sm *ShardMaster) {
	config := sm.newConfig()
	config.Shards[args.Shard] = args.GID
}

func (args *QueryArgs) execute(sm *ShardMaster) {
	// nothing to do for a read operation
}

func (sm *ShardMaster) newConfig() *Config {
	sm.configs = append(sm.configs, sm.configs[len(sm.configs)-1])
	config := &sm.configs[len(sm.configs)-1]
	config.Num++
	groups := make(map[int][]string)
	for gid, servers := range config.Groups {
		groups[gid] = servers
	}
	config.Groups = groups
	return config
}

func getCounts(ids []int) map[int]int {
	counts := make(map[int]int)
	for _, id := range ids {
		counts[id]++
	}
	return counts
}

func (config *Config) rebalance() {
	d := len(config.Groups)
	if d == 0 {
		return
	}
	gids := make([]int, d)
	i := 0
	for gid := range config.Groups {
		gids[i] = gid
		i++
	}
	counts := getCounts(config.Shards[:])
	sort.Slice(gids, func(i, j int) bool {
		return counts[gids[i]] < counts[gids[j]]
	})
	q, r := NShards/d, NShards%d
	for shard, gid := range config.Shards {
		if gid == 0 || counts[gid] > q+1 || counts[gid] == q+1 && r <= 0 {
			counts[gid]--
			config.Shards[shard] = gids[0]
			counts[gids[0]]++
			for i := 1; i < len(gids); i++ {
				if counts[gids[i-1]] <= counts[gids[i]] {
					break
				}
				gids[i-1], gids[i] = gids[i], gids[i-1]
			}
		}
		if counts[gid] == q+1 {
			r--
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.requestIds = make(map[int64]int64)
	sm.channels = make(map[string]chan struct{})

	labgob.Register(Op{})
	labgob.Register(&JoinArgs{})
	labgob.Register(&LeaveArgs{})
	labgob.Register(&MoveArgs{})
	labgob.Register(&QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	go func() {
		for applyMsg := range sm.applyCh {
			sm.mu.Lock()
			op := applyMsg.Command.(Op)
			if op.RequestId > sm.requestIds[op.ClientId] {
				op.Command.execute(sm)
				sm.requestIds[op.ClientId] = op.RequestId
			}
			if ch, ok := sm.channels[fmt.Sprintf("%d_%d", op.ClientId, op.RequestId)]; ok {
				close(ch)
			}
			sm.mu.Unlock()
		}
	}()

	return sm
}
