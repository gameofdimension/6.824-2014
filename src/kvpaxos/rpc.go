package kvpaxos

import (
	"fmt"
	"time"
)

// 两个重要参考文献
// http://css.csail.mit.edu/6.824/2014/labs/lab-3.html
// http://css.csail.mit.edu/6.824/2014/notes/l08-epaxos.txt

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

	for !kv.dead {
		kv.mu.Lock()
		if kv.lastApplied < instance {
			kv.mu.Unlock()
			time.Sleep(3 * time.Millisecond)
			continue
		}
		if lastSeq, ok := kv.lastClientSeq[clientId]; !ok || lastSeq != clientSeq {
			reply.Err = ErrSeqConflict
			kv.mu.Unlock()
			return nil
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
	copy := *args
	copy.Value = "*masked*"
	prefix := fmt.Sprintf("put handler me %d from client %d with args %v", kv.me, args.Id, copy)
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
	kv.px.Start(instance, op)
	DPrintf("%s start paxos %d", prefix, instance)

	for !kv.dead {
		kv.mu.Lock()
		if kv.lastApplied < instance {
			kv.mu.Unlock()
			time.Sleep(3 * time.Millisecond)
			continue
		}
		if lastSeq, ok := kv.lastClientSeq[clientId]; !ok || lastSeq != clientSeq {
			reply.Err = ErrSeqConflict
			kv.mu.Unlock()
			return nil
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

func (kv *KVPaxos) pollPaxos(instance int, targetOp Op) (bool, *Op) {
	to := 10 * time.Millisecond
	for !kv.dead {
		decided, value := kv.px.Status(instance)
		copy := targetOp
		copy.Value = "*masked*"
		DPrintf("me %d poll status of %d expected %v", kv.me, instance, copy)
		if decided {
			agreement := value.(Op)
			if agreement == targetOp {
				return true, &agreement
			} else {
				return false, &agreement
			}
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return false, nil
}

func (kv *KVPaxos) fillGap(seq int) {
	op := Op{
		Type: OpNoop,
	}
	kv.px.Start(seq, op)
	DPrintf("me %d fill gap seq %d with noop started", kv.me, seq)
	ok, actual := kv.pollPaxos(seq, op)
	copy := actual
	copy.Value = "*masked*"
	DPrintf("me %d fill gap seq %d with noop result %t, %v", kv.me, seq, ok, copy)
}
