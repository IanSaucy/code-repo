package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
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
