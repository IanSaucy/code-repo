package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "sync/atomic"

var seqNo int64 = 1 // 唯一编号

func nextval(addr *int64) int64 {
	return atomic.AddInt64(addr, 1)
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	clientId int64 // 客户端id
	leader int // 上一次发送RPC的leader，默认初始化为随机数
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

	ck.clientId = nextval(&seqNo)
	ck.leader = int(nrand()) % len(ck.servers)
	
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := GetArgs{
		Key : key,
		ClientId : ck.clientId,
		OpNo : nextval(&seqNo),
	}
	serverLen := len(ck.servers)

	for {
		var reply GetReply
		
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.WrongLeader {
				// 不是leader
				ck.leader = (ck.leader + 1) % serverLen
			} else {
				// 是leader
			}
			
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			} else {
				mylog("Get[", args.OpNo, "]发生错误,", reply.Err)
			}
		} else {
			// RPC未正常返回
		}
	}
	
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{
		Key : key,
		Value : value,
		Op : op,
		ClientId : ck.clientId,
		OpNo : nextval(&seqNo),
	}
	serverLen := len(ck.servers)

	for {
		var reply PutAppendReply
		
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.WrongLeader {
				// 不是leader
				ck.leader = (ck.leader + 1) % serverLen
			} else {
				// 是leader
			}
			
			if reply.Err == OK {
				return reply.Value
			} else {
				mylog(op, "[", args.OpNo, "]发生错误,", reply.Err)
			}
		} else {
			// RPC未正常返回
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
