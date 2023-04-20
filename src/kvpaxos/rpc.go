package kvpaxos

import (
	"fmt"
	"strconv"
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
		ClientId:  clientId,
		ClientSeq: clientSeq,
		Type:      OpGet,
		Key:       args.Key,
	}
	kv.px.Start(instance, op)
	rc := kv.pollPaxos(instance, op)
	if !rc {
		reply.Err = ErrSeqConflict
		return nil
	}
	kv.appleyLog(instance)
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
		Type:      OpPut,
		ClientId:  clientId,
		ClientSeq: clientSeq,
		Key:       args.Key,
		Value:     args.Value,
		DoHash:    args.DoHash,
	}
	kv.px.Start(instance, op)
	rc := kv.pollPaxos(instance, op)
	if !rc {
		reply.Err = ErrSeqConflict
		return nil
	}
	kv.appleyLog(instance)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if lastSeq, ok := kv.lastClientSeq[clientId]; !ok || lastSeq != clientSeq {
		panic(fmt.Sprintf("put handler me %d client %d expected %d got %d", kv.me, clientId, clientSeq, lastSeq))
	}
	reply.Err = OK
	if args.DoHash {
		reply.PreviousValue = kv.lastClientResult[clientId].(string)
	}
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
	DPrintf("try agree seq %d with noop, result %t", seq, ok)
}

func (kv *KVPaxos) appleyLog(seq int) {
	for i := kv.lastApply + 1; i <= seq; i += 1 {
		decided, val := kv.px.Status(i)
		if !decided {
			// todo fill gap
			kv.fillGap(i)
			decided, val = kv.px.Status(i)
			if !decided {
				panic(fmt.Sprintf("seq %d not decided after fill gap", i))
			}
		}
		kv.mu.Lock()
		op := val.(Op)
		lastSeq, ok := kv.lastClientSeq[op.ClientId]
		if !ok || lastSeq != op.ClientSeq {
			if op.ClientId < lastSeq {
				panic(fmt.Sprintf("smaller seq %d vs %d for %d", op.ClientId, lastSeq, op.ClientId))
			}
			if op.Type == OpPut {
				key := op.Key
				value := op.Value
				previous := kv.repo[key]
				kv.lastClientSeq[op.ClientId] = op.ClientSeq
				if op.DoHash {
					h := hash(previous + value)
					value = strconv.Itoa(int(h))
					kv.lastClientResult[op.ClientId] = previous
				} else {
					kv.lastClientResult[op.ClientId] = true
				}
				kv.repo[key] = value
			} else if op.Type == OpGet {
				key := op.Key
				kv.lastClientSeq[op.ClientId] = op.ClientSeq
				if value, ok := kv.repo[key]; ok {
					kv.lastClientResult[op.ClientId] = value
				} else {
					kv.lastClientResult[op.ClientId] = nil
				}
			}
		}
		kv.lastApply = i
		kv.mu.Unlock()
		kv.px.Done(i)
		kv.px.Min()
	}
}
