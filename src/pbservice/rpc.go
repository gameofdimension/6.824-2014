package pbservice

import (
	"fmt"
	"strconv"
)

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	prefix := fmt.Sprintf("get handler me %s is primary %t with %v", pb.me, pb.isPrimary(), *args)
	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		DPrintf("%s not primary", prefix)
		return nil
	}
	req := SyncArgs{
		Op:      SyncTypeGet,
		Args:    *args,
		Viewnum: pb.viewNum,
	}
	if !pb.forwardToBackup(&req) {
		reply.Err = ErrWrongView
		DPrintf("%s forward fail", prefix)
		return nil
	}
	client := args.Id
	seq := args.Seq
	if lastSeq, ok := pb.lastClientSeq[client]; ok {
		if seq < lastSeq {
			panic(fmt.Sprintf("Get client %d seq out of order %d vs %d", client, seq, lastSeq))
		}
		if seq == lastSeq {
			value := pb.lastClientResult[client]
			if value == nil {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
				reply.Value = value.(string)
			}
			DPrintf("%s return with cached value %v", prefix, reply.Value)
			return nil
		}
	}
	if value, ok := pb.repo[args.Key]; ok {
		pb.lastClientSeq[client] = seq
		pb.lastClientResult[client] = value
		reply.Err = OK
		reply.Value = value
	} else {
		pb.lastClientSeq[client] = seq
		pb.lastClientResult[client] = nil
		reply.Err = ErrNoKey
	}
	DPrintf("%s get ok", prefix)
	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	prefix := fmt.Sprintf("put handler me %s is primary %t with %v", pb.me, pb.isPrimary(), *args)
	DPrintf("%s", prefix)
	if !pb.isPrimary() {
		DPrintf("%s not primary", prefix)
		reply.Err = ErrWrongServer
		return nil
	}
	req := SyncArgs{
		Op:      SyncTypePut,
		Args:    *args,
		Viewnum: pb.viewNum,
	}
	if !pb.forwardToBackup(&req) {
		DPrintf("%s forward fail", prefix)
		reply.Err = ErrWrongView
		return nil
	}
	client := args.Id
	seq := args.Seq
	key := args.Key
	value := args.Value
	if args.DoHash {
		h := hash(pb.repo[key] + value)
		value = strconv.Itoa(int(h))
	}
	if lastSeq, ok := pb.lastClientSeq[client]; ok {
		if seq < lastSeq {
			panic(fmt.Sprintf("Put client %d seq out of order %d vs %d", client, seq, lastSeq))
		}
		if seq == lastSeq {
			value := pb.lastClientResult[client]
			if args.DoHash {
				reply.PreviousValue = value.(string)
			}
			reply.Err = OK
			DPrintf("%s return with cached value %v", prefix, value)
			return nil
		}
	}
	prev := pb.repo[key]
	pb.lastClientSeq[client] = seq
	reply.Err = OK
	if args.DoHash {
		pb.lastClientResult[client] = prev
		reply.PreviousValue = prev
	} else {
		pb.lastClientResult[client] = true
	}
	pb.repo[key] = value
	DPrintf("%s put ok", prefix)
	return nil
}

func (pb *PBServer) Forward(args *SyncArgs, reply *SyncReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	prefix := fmt.Sprintf("forward handler me %s is primary %t with %v", pb.me, pb.isPrimary(), *args)
	if args.Viewnum < pb.viewNum {
		reply.Err = ErrWrongView
		DPrintf("%s req view smaller %d vs %d", prefix, args.Viewnum, pb.viewNum)
		return nil
	}
	if args.Viewnum > pb.viewNum {
		reply.Err = ErrFutureView
		DPrintf("%s req view greater %d vs %d", prefix, args.Viewnum, pb.viewNum)
		return nil
	}
	if pb.backup != pb.me {
		panic(fmt.Sprintf("same view %d, different perception [%s, %s]", pb.viewNum, pb.primary, pb.backup))
	}
	if args.Op == SyncTypeGet {
		getArgs := args.Args.(GetArgs)
		client := getArgs.Id
		seq := getArgs.Seq
		key := getArgs.Key
		pb.lastClientSeq[client] = seq
		if val, ok := pb.repo[key]; ok {
			pb.lastClientResult[client] = val
		} else {
			pb.lastClientResult[client] = nil
		}
		reply.Err = OK
		DPrintf("%s apply get op %v", prefix, getArgs)
		return nil
	} else {
		putArgs := args.Args.(PutArgs)
		client := putArgs.Id
		seq := putArgs.Seq
		key := putArgs.Key
		value := putArgs.Value
		if putArgs.DoHash {
			h := hash(pb.repo[key] + value)
			value = strconv.Itoa(int(h))
		}
		if lastSeq, ok := pb.lastClientSeq[client]; ok {
			if seq < lastSeq {
				panic(fmt.Sprintf("Forward client %d seq out of order %d vs %d", client, seq, lastSeq))
			}
			if seq == lastSeq {
				reply.Err = OK
				return nil
			}
		}
		prev := pb.repo[key]
		pb.lastClientSeq[client] = seq
		reply.Err = OK
		if putArgs.DoHash {
			pb.lastClientResult[client] = prev
		} else {
			pb.lastClientResult[client] = true
		}
		pb.repo[key] = value
		DPrintf("%s apply put op %v", prefix, putArgs)
		return nil
	}
}

func (pb *PBServer) Dump(args *DumpArgs, reply *DumpReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	prefix := fmt.Sprintf("forward handler me %s is primary %t with %v", pb.me, pb.isPrimary(), *args)
	if args.Viewnum < pb.viewNum {
		reply.Err = ErrWrongView
		DPrintf("%s req view smaller %d vs %d", prefix, args.Viewnum, pb.viewNum)
		return nil
	}
	if args.Viewnum > pb.viewNum {
		reply.Err = ErrFutureView
		DPrintf("%s req view greater %d vs %d", prefix, args.Viewnum, pb.viewNum)
		return nil
	}
	if pb.backup != pb.me {
		panic(fmt.Sprintf("same view %d, different perception [%s, %s]", pb.viewNum, pb.backup, pb.me))
	}

	reply.Err = OK
	pb.repo = args.Repo
	pb.lastClientSeq = args.LastClientSeq
	pb.lastClientResult = args.LastClientResult
	DPrintf("%s dump ok", prefix)
	return nil
}
