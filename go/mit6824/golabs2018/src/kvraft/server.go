package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const mydebugEnabled = false
const mylogEnabled = false
const DEBUG_KVSERVER = false
const DEBUG_GET = true && DEBUG_KVSERVER
const DEBUG_PUTAPPEND = true && DEBUG_KVSERVER
const DEBUG_APPLYMSG = false && DEBUG_KVSERVER

func mydebug(a ...interface{}) (n int, err error) {
	if mydebugEnabled {
		n, err = fmt.Println(a...)
	}
	return
}
func mylog(a ...interface{}) (n int, err error) {
	if mylogEnabled {
		n, err = fmt.Println(a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Op       string // "Put" or "Append" or "Get"
	ClientId int64  // 客户端编号
	OpNo     int64  // 唯一操作编号
	Key      string
	Value    string
}

type OpReply struct {
	ClientId int64
	OpNo     int64
	Term  int
	Index int
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	killCh chan int

	kvMap map[string]string // 保存成功通过Raft的数据

	// clientId -> 操作结果
	// 由于一个客户端一次只会发起一次请求，但是可能重复发送
	lastClientOpMap map[int64]*OpReply
	// CommandIndex -> 请求
	lastCommandMap map[int]chan OpReply
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}
func (kv *KVServer) Unlock() {
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if DEBUG_GET {
		mylog("KVServer[", kv.me, "]接收到", OpStructToString(args))
	}

	opReply := kv.tryOp("Get", args.ClientId, args.OpNo, args.Key, "")
	reply.WrongLeader = opReply.Err == WrongLeader
	reply.Err = opReply.Err
	reply.Value = opReply.Value

	if DEBUG_GET {
		mylog("KVServer[", kv.me, "]接收到", OpStructToString(args), "返回", OpStructToString(reply))
	}
}

func (kv *KVServer) tryOp(op string, clientId int64, opNo int64, key string, value string) OpReply {
	// 检查一下跟之前的是不是同一个，是同一个直接返回
	kv.Lock()
	if opReply, ok := kv.lastClientOpMap[clientId]; ok && opReply.ClientId == clientId && opReply.OpNo == opNo {
		kv.Unlock()
		mylog("KVServer[", kv.me, "]是已经存在ClientId[", clientId, "]Op[", opNo, "]")
		return *opReply
	}
	kv.Unlock()

	cmd := Op{
		Op:       op,
		ClientId: clientId,
		OpNo:     opNo,
		Key:      key,
		Value:    value,
	}
	index, term, isLeader := kv.rf.Start(cmd)
	mylog("KVServer[", kv.me, "]发起命令Client[", clientId, "]Op[", opNo, "]", op, ",", key, ",", value, ",任期[", term, "]日志索引[", index, "]")

	if !isLeader {
		return OpReply{Err: WrongLeader}
	} else {
		mylog("KVServer[", kv.me, "]是Leader")
		
		notifyCh := make(chan OpReply)
		kv.Lock()
		kv.lastCommandMap[index] = notifyCh
		kv.Unlock()

		select {
		case <-time.After(time.Duration(RaftStartTimeout) * time.Millisecond):
			mylog("KVServer[", kv.me, "]Start之后超时了")
			kv.Lock()
			delete(kv.lastCommandMap, index)
			kv.Unlock()
			return OpReply{Err:KVServerTimeout}
		case <-kv.killCh:
			// 结束
			return OpReply{Err: KVServerKilled}
		case opReply := <- notifyCh:
			// 收到通知
			if opReply.Term != term {
				return OpReply{Err: WrongLeader}
			} else {
				return opReply
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if DEBUG_PUTAPPEND {
		mylog("KVServer[", kv.me, "]接收到", OpStructToString(args))
	}

	opReply := kv.tryOp(args.Op, args.ClientId, args.OpNo, args.Key, args.Value)
	reply.WrongLeader = opReply.Err == WrongLeader
	reply.Err = opReply.Err

	if DEBUG_PUTAPPEND {
		mylog("KVServer[", kv.me, "]接收到", OpStructToString(args), "返回", OpStructToString(reply))
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.

	mylog("KVServer[", kv.me, "]Killed")
	close(kv.killCh)
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.killCh = make(chan int, 1)
	kv.kvMap = make(map[string]string)
	kv.lastClientOpMap = make(map[int64]*OpReply)
	kv.lastCommandMap = make(map[int]chan OpReply)

	go kv.applyCommandThread()

	return kv
}

func (kv *KVServer) applyCommandThread() {
	running := true

	mylog("KVServer[", kv.me, "]开始applyCommandThread")

	for running {
		select {
		case <-kv.killCh:
			// 关闭了
			running = false
		case msg := <-kv.applyCh:
			// 收到ApplyMsg
			kv.applyCommand(msg)
		}
	}

	mylog("KVServer[", kv.me, "]结束applyCommandThread")
}
func (kv *KVServer) applyCommand(msg raft.ApplyMsg) {
	if DEBUG_APPLYMSG {
		mylog("KVServer[", kv.me, "]接收到ApplyMsg", msg)
	}

	if msg.CommandValid {
		kv.applyValidCommand(msg)
	} else {
		// 其他
	}
}
func (kv *KVServer) isSameWithPrevious(op Op) bool {
	if opResult, ok := kv.lastClientOpMap[op.ClientId]; ok {
		return opResult.OpNo == op.OpNo
	} else {
		return false
	}
}
func (kv *KVServer) applyValidCommand(msg raft.ApplyMsg) {
	kv.Lock()
	defer kv.Unlock()

	cmd := msg.Command.(Op)
	opReply := OpReply{ClientId : cmd.ClientId, OpNo : cmd.OpNo,  Term : msg.CommandTerm, Index : msg.CommandIndex, Err : OK, Value : ""}

	// 客户端同样的命令有可能执行了多次，Get要以最后一次为主,Put/Append则必须只执行一次
	if cmd.Op == OpGet {
			if oldValue, ok := kv.kvMap[cmd.Key]; ok {
				// 存在
				opReply.Value = oldValue
			} else {
				// 不存在
				opReply.Err = ErrNoKey
				opReply.Value = ""
			}
			//mylog("KVServer[", kv.me, "]Get(", cmd.Key, ")")
	}
	
	if !kv.isSameWithPrevious(cmd)  {
		 if cmd.Op == OpPut {
			kv.kvMap[cmd.Key] = cmd.Value
			//mylog("KVServer[", kv.me, "]Put(", cmd.Key, ", ", cmd.Value, ")")
		} else {
			if oldValue, ok := kv.kvMap[cmd.Key]; ok {
				kv.kvMap[cmd.Key] = oldValue + cmd.Value
			} else {
				kv.kvMap[cmd.Key] = cmd.Value
			}
			//mylog("KVServer[", kv.me, "]Append(", cmd.Key, ", ", kv.kvMap[cmd.Key], ")")
		}
		kv.lastClientOpMap[cmd.ClientId] = &opReply
	} else {
		mylog("命令Client[", cmd.ClientId, "]Op[", cmd.OpNo, "]已经执行，不用再次执行")
	}

	if false {
		if oldValue, ok := kv.kvMap[cmd.Key]; ok {
			mylog("存在Key:", cmd.Key, "，Value:", oldValue)
		} else {
			mylog("不存在Key:", cmd.Key)
		}
	}

	if notifyCh, ok := kv.lastCommandMap[msg.CommandIndex]; ok {
		//mylog("lastCommandMap中存在日志索引[", msg.CommandIndex, "]")
		
		go func() {
			notifyCh <- opReply
		}()

		delete(
		kv.lastCommandMap, msg.CommandIndex)
		//mylog("KVServer[", kv.me, "]中lastCommandMap删除", msg.CommandIndex, "成功")
	} else {
		//mylog("lastCommandMap中不存在日志索引[", msg.CommandIndex, "]")
	}

}
