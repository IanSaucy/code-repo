package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"
import "time"
import "raft"

const DEBUG_CLIENT = true

var seqNo int64 = 1 // 唯一编号

func nextval(addr *int64) int64 {
	return atomic.AddInt64(addr, 1)
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	clientId int64 // 客户端id
	leader   int   // 上一次发送RPC的leader，默认初始化为随机数
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

// 返回true表示成功结束, 否则继续循环
func (ck *Clerk) tryOp(op string, args interface{}) interface{} {
	startLeader := ck.leader

	mylog("开始发送请求", OpStructToString(args))

	for {
		var reply interface{}

		if op == KVServerGet {
			reply = &GetReply{}
		} else {
			reply = &PutAppendReply{}
		}

		ch := make(chan bool)

		go func(leader int) {
			if DEBUG_CLIENT {
				mylog("向KVServer[", leader, "]开始发送请求", OpStructToString(args))
			}
			ok := ck.servers[leader].Call(op, args, reply)
			if DEBUG_CLIENT {
				mylog("向KVServer[", leader, "]结束发送请求", OpStructToString(reply))
			}
			ch <- ok
		}(ck.leader)

		select {
		case <-time.After(time.Duration(KVServerRPCTimeout) * time.Millisecond):
			// 超时了，继续
		case ok := <-ch:
			// 收到RPC返回
			var err Err

			if op == KVServerGet {
				tmpArgs := reply.(*GetReply)
				err = tmpArgs.Err
			} else {
				tmpArgs := reply.(*PutAppendReply)
				err = tmpArgs.Err
			}

			if ok {
				if err == OK || err == ErrNoKey || err == KVServerKilled {
					mylog("结束发送请求", OpStructToString(args))
					return reply
				} else if err == WrongLeader {
					// 不是leader，继续
				}  else {
					mylog(OpStructToString(args), "发生未知错误[", err, "]")
				}

			} else {
				// RPC未正常返回
				mylog("RPC Call未正常返回")
			}
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
		if ck.leader == startLeader { // 走了一圈
			time.Sleep(time.Duration(raft.MAX_ELECTION_TIMEOUT) * time.Millisecond)
		}
	}
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
		Key:      key,
		ClientId: ck.clientId,
		OpNo:     nextval(&seqNo),
	}

	reply := ck.tryOp(KVServerGet, &args)
	return reply.(*GetReply).Value
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
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		OpNo:     nextval(&seqNo),
	}

	ck.tryOp(KVServerPutAppend, &args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
