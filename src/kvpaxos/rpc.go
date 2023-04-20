package kvpaxos

import (
	"fmt"
	"time"
)

const (
	OpGet = 1
	OpPut = 2
)

type OpType uint64
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type   OpType
	Key    string
	Value  string
	DoHash bool
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	clientId := args.Id
	clientSeq := args.Seq
	kv.mu.Lock()
	prefix := fmt.Sprintf("kv Get me %d from client %d with args %v", kv.me, args.Id, *args)
	if lastSeq, ok := kv.lastClientSeq[clientId]; ok {
		if clientSeq < lastSeq {
			panic(fmt.Sprintf("%s seq out of order %d vs %d", prefix, clientSeq, lastSeq))
		}
		if clientSeq == lastSeq {
			val := kv.lastClientResult[clientId]
			if val == nil {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
				reply.Value = val.(string)
			}
			kv.mu.Unlock()
			return nil
		}
	}
	kv.mu.Unlock()

	instance := kv.px.Max() + 1
	op := Op{
		Type: OpGet,
		Key:  args.Key,
	}
	kv.px.Start(instance, op)
	rc := kv.pollPaxos(instance, op)
	if !rc {
		reply.Err = ErrSeqConflict
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// todo ensure all previous instance finished
	kv.lastClientSeq[clientId] = clientSeq
	if result, ok := kv.repo[args.Key]; ok {
		kv.lastClientResult[clientId] = result
		reply.Err = OK
		reply.Value = result
	} else {
		kv.lastClientResult[clientId] = nil
		reply.Err = ErrNoKey
	}
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	clientId := args.Id
	clientSeq := args.Seq
	kv.mu.Lock()
	prefix := fmt.Sprintf("kv Put me %d from client %d with args %v", kv.me, args.Id, *args)
	if lastSeq, ok := kv.lastClientSeq[clientId]; ok {
		if clientSeq < lastSeq {
			panic(fmt.Sprintf("%s seq out of order %d vs %d", prefix, clientSeq, lastSeq))
		}
		if clientSeq == lastSeq {
			val := kv.lastClientResult[clientId]
			if args.DoHash {
				reply.PreviousValue = val.(string)
			}
			reply.Err = OK
			kv.mu.Unlock()
			return nil
		}
	}
	kv.mu.Unlock()
	instance := kv.px.Max() + 1
	op := Op{
		Type:   OpPut,
		Key:    args.Key,
		Value:  args.Value,
		DoHash: args.DoHash,
	}
	kv.px.Start(instance, op)
	rc := kv.pollPaxos(instance, op)
	if !rc {
		reply.Err = ErrSeqConflict
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// todo ensure all previous instance finished
	return nil
}

func (kv *KVPaxos) pollPaxos(instance int, targetOp Op) bool {
	to := 10 * time.Millisecond
	for !kv.dead {
		decided, value := kv.px.Status(instance)
		if decided {
			agreement := value.(Op)
			if agreement == targetOp {
				return true
			} else {
				return false
			}
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return false
}
