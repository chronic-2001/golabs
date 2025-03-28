package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"golabs/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.requestId++
	args := GetArgs{key, ck.clientId, ck.requestId}
	var reply GetReply
	for !ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) || reply.Err == ErrWrongLeader {
		reply = GetReply{}
		ck.leaderId++
		if ck.leaderId == len(ck.servers) {
			ck.leaderId = 0
			time.Sleep(100 * time.Millisecond)
		}
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestId++
	args := PutAppendArgs{key, value, op, ck.clientId, ck.requestId}
	var reply PutAppendReply
	for !ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) || reply.Err == ErrWrongLeader {
		reply = PutAppendReply{}
		ck.leaderId++
		if ck.leaderId == len(ck.servers) {
			ck.leaderId = 0
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
