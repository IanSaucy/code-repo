package raftkv

import "fmt"

const (
	// 全部Err定义
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	KVServerKilled = "KVServerKilled"
	WrongLeader = "WrongLeader"
	

	// RPC的名字
	KVServerGet = "KVServer.Get"
	KVServerPutAppend = "KVServer.PutAppend"

	// KVServer RPC超时时间
	KVServerRPCTimeout = 200
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientId int64 // 客户端编号
	OpNo int64 // 唯一操作编号
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClientId int64 // 客户端编号
	OpNo int64 // 唯一操作编号
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

func BoolToString(v bool) string {
	if v {
		return "true"
	} else {
		return "false"
	}
}

func OpStructToString(cmd interface{}) string {
	if args, ok := cmd.(*GetArgs); ok {
		return fmt.Sprintf("Client[%d]Op[%d]Get(%s)", args.ClientId, args.OpNo, args.Key)
	} else if args, ok := cmd.(*PutAppendArgs); ok {
		return fmt.Sprintf("Client[%d]Op[%d]%s(%s, %s)", args.ClientId, args.OpNo, args.Op, args.Key, args.Value)
	} else if args, ok := cmd.(GetArgs); ok {
		return fmt.Sprintf("Client[%d]Op[%d]Get(%s)", args.ClientId, args.OpNo, args.Key)
	} else if args, ok := cmd.(PutAppendArgs); ok {
		return fmt.Sprintf("Client[%d]Op[%d]%s(%s, %s)", args.ClientId, args.OpNo, args.Op, args.Key, args.Value)
	} else if args, ok := cmd.(*GetReply); ok {
		return fmt.Sprintf("GetReply(%s, %s, %s)", BoolToString(args.WrongLeader), args.Err, args.Value)
	} else if args, ok := cmd.(*PutAppendReply); ok {
		return fmt.Sprintf("PutAppendReply(%s, %s)", BoolToString(args.WrongLeader), args.Err)
	}
	return ""
}
