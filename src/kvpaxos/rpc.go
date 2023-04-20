package kvpaxos

import (
	"fmt"
	"time"
)

const (
	OpNoop = 0
	OpGet  = 1
	OpPut  = 2
)

type OpType uint64
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      OpType
	ClientId  int64
	ClientSeq int64
	Key       string
	Value     string
	DoHash    bool
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	clientId := args.Id
	clientSeq := args.Seq
	kv.mu.Lock()
	prefix := fmt.Sprintf("get handler me %d from client %d with args %v", kv.me, args.Id, *args)
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
			DPrintf("%s return with cached value %v", prefix, reply)
			kv.mu.Unlock()
			return nil
		}
	}
	kv.mu.Unlock()

	instance := kv.px.Max() + 1
	op := Op{
		Type:      OpGet,
		ClientId:  clientId,
		ClientSeq: clientSeq,
		Key:       args.Key,
	}
	DPrintf("%s start paxos %d", prefix, instance)
	kv.px.Start(instance, op)
	rc := kv.pollPaxos(instance, op)
	if !rc {
		DPrintf("%s reach agreement at %d fail", prefix, instance)
		reply.Err = ErrSeqConflict
		return nil
	}
	DPrintf("%s reach agreement at %d succeed", prefix, instance)
	kv.mu.Lock()
	if instance > kv.lastAgree {
		kv.lastAgree = instance
	}
	kv.mu.Unlock()

	for !kv.dead {
		kv.mu.Lock()
		if kv.lastApply < instance {
			time.Sleep(3 * time.Millisecond)
			kv.mu.Unlock()
			continue
		}
		if lastSeq, ok := kv.lastClientSeq[clientId]; !ok || lastSeq != clientSeq {
			panic(fmt.Sprintf("get handler me %d client %d expected %d got %d", kv.me, clientId, clientSeq, lastSeq))
		}
		cached := kv.lastClientResult[clientId]
		if cached == nil {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = cached.(string)
		}
		kv.mu.Unlock()
		return nil
	}
	reply.Err = ErrKilled
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	clientId := args.Id
	clientSeq := args.Seq
	kv.mu.Lock()
	prefix := fmt.Sprintf("put handler me %d from client %d with args %v", kv.me, args.Id, *args)
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
		Type:      OpPut,
		ClientId:  clientId,
		ClientSeq: clientSeq,
		Key:       args.Key,
		Value:     args.Value,
		DoHash:    args.DoHash,
	}
	DPrintf("%s start paxos %d", prefix, instance)
	kv.px.Start(instance, op)
	rc := kv.pollPaxos(instance, op)
	if !rc {
		reply.Err = ErrSeqConflict
		return nil
	}
	kv.mu.Lock()
	if instance > kv.lastAgree {
		kv.lastAgree = instance
	}
	kv.mu.Unlock()

	for !kv.dead {
		kv.mu.Lock()
		if kv.lastApply < instance {
			time.Sleep(3 * time.Millisecond)
			kv.mu.Unlock()
			continue
		}
		if lastSeq, ok := kv.lastClientSeq[clientId]; !ok || lastSeq != clientSeq {
			panic(fmt.Sprintf("put handler me %d client %d expected %d got %d", kv.me, clientId, clientSeq, lastSeq))
		}
		reply.Err = OK
		if args.DoHash {
			reply.PreviousValue = kv.lastClientResult[clientId].(string)
		}
		kv.mu.Unlock()
		return nil
	}
	reply.Err = ErrKilled
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

func (kv *KVPaxos) fillGap(seq int) {
	op := Op{
		Type: OpNoop,
	}
	kv.px.Start(seq, op)
	ok := kv.pollPaxos(seq, op)
	DPrintf("me %d try agree seq %d with noop, result %t", kv.me, seq, ok)
}
