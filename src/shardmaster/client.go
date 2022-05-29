package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId  int64
	requestId int64
	leaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.requestId++
	args := QueryArgs{num, ck.clientId, ck.requestId}
	var reply QueryReply
	for !ck.servers[ck.leaderId].Call("ShardMaster.Query", &args, &reply) || reply.WrongLeader {
		reply = QueryReply{}
		ck.leaderId++
		if ck.leaderId == len(ck.servers) {
			ck.leaderId = 0
			time.Sleep(100 * time.Millisecond)
		}
	}
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.requestId++
	args := JoinArgs{servers, ck.clientId, ck.requestId}
	var reply JoinReply
	for !ck.servers[ck.leaderId].Call("ShardMaster.Join", &args, &reply) || reply.WrongLeader {
		reply = JoinReply{}
		ck.leaderId++
		if ck.leaderId == len(ck.servers) {
			ck.leaderId = 0
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.requestId++
	args := LeaveArgs{gids, ck.clientId, ck.requestId}
	var reply LeaveReply
	for !ck.servers[ck.leaderId].Call("ShardMaster.Leave", &args, &reply) || reply.WrongLeader {
		reply = LeaveReply{}
		ck.leaderId++
		if ck.leaderId == len(ck.servers) {
			ck.leaderId = 0
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.requestId++
	args := MoveArgs{shard, gid, ck.clientId, ck.requestId}
	var reply MoveReply
	for !ck.servers[ck.leaderId].Call("ShardMaster.Move", &args, &reply) || reply.WrongLeader {
		reply = MoveReply{}
		ck.leaderId++
		if ck.leaderId == len(ck.servers) {
			ck.leaderId = 0
			time.Sleep(100 * time.Millisecond)
		}
	}
}
