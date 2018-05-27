package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
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

const OP_STATE_STARTED = 1  // 已经调用了raft的Start，但是没有收到响应
const OP_STATE_APPLIED = 2  // 收到了raft的apply消息
const OP_STATE_REPLACED = 3 // 提交的命令的日志索引被其他命令占去了

type OpReply struct {
	Term int

	WrongLeader bool
	Err         Err
	Value       string
}

type OpResult struct {
	ClientId int64
	OpNo     int64 // 唯一操作编号

	Term         int
	CommandIndex int

	NotifyCh  chan OpReply // 通知其他等待的
	WaitCount int          // 等待的个数计数

	reply OpReply
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
	lastClientOpMap map[int64]*OpResult
	// CommandIndex -> 请求
	lastCommandMap map[int]*OpResult
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
	reply.WrongLeader = opReply.WrongLeader
	reply.Err = opReply.Err
	reply.Value = opReply.Value

	if DEBUG_GET {
		mylog("KVServer[", kv.me, "]接收到", OpStructToString(args), "返回", OpStructToString(reply))
	}
}

func (kv *KVServer) tryOp(op string, clientId int64, opNo int64, key string, value string) OpReply {
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
		return OpReply{WrongLeader: true, Err: WrongLeader}
	} else {
		opRes := &OpResult{
			ClientId:     clientId,
			OpNo:         opNo,
			Term:         term,
			CommandIndex: index,
			NotifyCh:     make(chan OpReply),
		}
		kv.Lock()
		kv.lastCommandMap[index] = opRes
		kv.Unlock()

		select {
		case <-kv.killCh:
			// 结束
			return OpReply{WrongLeader: true, Err: KVServerKilled}
		case opReply := <-opRes.NotifyCh:
			// 收到通知
			if opReply.Term != term {
				return OpReply{WrongLeader: true, Err: WrongLeader}
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
	reply.WrongLeader = opReply.WrongLeader
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
	kv.lastClientOpMap = make(map[int64]*OpResult)
	kv.lastCommandMap = make(map[int]*OpResult)

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
	var opReply OpReply

	opReply.Term = msg.CommandTerm

	// 检查跟上一次命令是否相同，相同则不做处理
	if !kv.isSameWithPrevious(cmd) {
		switch cmd.Op {
		case "Get":
			if oldValue, ok := kv.kvMap[cmd.Key]; ok {
				// 存在
				opReply.Value = oldValue
				opReply.Err = OK
			} else {
				// 不存在
				opReply.Err = ErrNoKey
				opReply.Value = ""
			}
			mylog("KVServer[", kv.me, "]Get(", cmd.Key, ")")
		case "Put":
			kv.kvMap[cmd.Key] = cmd.Value
			opReply.Err = OK
			mylog("KVServer[", kv.me, "]Put(", cmd.Key, ", ", cmd.Value, ")")
		case "Append":
			if oldValue, ok := kv.kvMap[cmd.Key]; ok {
				kv.kvMap[cmd.Key] = oldValue + cmd.Value
			} else {
				kv.kvMap[cmd.Key] = cmd.Value
			}
			opReply.Err = OK
			mylog("KVServer[", kv.me, "]Append(", cmd.Key, ", ", cmd.Value, ")")
		}

		opRes := &OpResult{
			ClientId:     cmd.ClientId,
			OpNo:         cmd.OpNo,
			Term:         msg.CommandTerm,
			CommandIndex: msg.CommandIndex,
		}
		kv.lastClientOpMap[cmd.ClientId] = opRes
	}

	if false {
		if oldValue, ok := kv.kvMap[cmd.Key]; ok {
			mylog("存在Key:", cmd.Key, "，Value:", oldValue)
		} else {
			mylog("不存在Key:", cmd.Key)
		}
	}

	if opResult, ok := kv.lastCommandMap[msg.CommandIndex]; ok {
		//mylog("lastCommandMap中存在日志索引[", msg.CommandIndex, "]")

		switch cmd.Op {
		case "Get":
			if oldValue, ok := kv.kvMap[cmd.Key]; ok {
				// 存在
				opReply.Value = oldValue
				opReply.Err = OK
			} else {
				// 不存在
				opReply.Err = ErrNoKey
				opReply.Value = ""
			}
		default:
			opReply.Err = OK
		}

		go func() {
			opResult.NotifyCh <- opReply
		}()

		delete(kv.lastCommandMap, msg.CommandIndex)
		mylog("KVServer[", kv.me, "]中lastCommandMap删除", msg.CommandIndex, "成功")
	} else {
		//mylog("lastCommandMap中不存在日志索引[", msg.CommandIndex, "]")
	}

}
