package pbservice

import "hash/fnv"

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongServer  = "ErrWrongServer"
	ErrWrongView    = "ErrWrongView"
	ErrFutureView   = "ErrFutureView"
	ErrObsoleteView = "ErrObsoleteView"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  int64
	Seq int64
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id  int64
	Seq int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type SyncType int

const (
	SyncTypeGet = 1
	SyncTypePut = 2
)

type SyncArgs struct {
	Op      SyncType
	Args    interface{}
	Viewnum uint
}

type SyncReply struct {
	Err Err
}

type DumpArgs struct {
	Viewnum          uint
	LastClientSeq    map[int64]int64
	LastClientResult map[int64]interface{}
	Repo             map[string]string
}

type DumpReply struct {
	Err Err
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
