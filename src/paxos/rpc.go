package paxos

import "fmt"

const (
	OK        = "OK"
	ErrReject = "ErrReject"
)

type Err string

type PrepareArgs struct {
	Caller  int
	MaxDone int
	Seq     int
	N       int
}

type PrepareReply struct {
	MaxDone int
	Err     Err
	NA      int
	VA      interface{}
}

type AcceptArgs struct {
	Caller  int
	MaxDone int
	Seq     int
	N       int
	V       interface{}
}

type AcceptReply struct {
	MaxDone int
	Err     Err
}

type DecideArgs struct {
	Caller  int
	MaxDone int
	Seq     int
	V       interface{}
}

type DecideReply struct {
	MaxDone int
	Err     Err
}

func (px *Paxos) findOrCreate(seq int, value interface{}, status Status) *Instance {
	px.mu.Lock()
	defer px.mu.Unlock()
	inst, ok := px.seqToInstance[seq]
	if !ok {
		tmp := MakeInstance(seq, value, len(px.peers), px.me, status)
		inst = &tmp
		px.seqToInstance[seq] = inst
	}
	if seq > px.maxKnown {
		px.maxKnown = seq
	}
	return inst
}

func (px *Paxos) updateMaxDone(server int, maxDone int) int {
	px.mu.Lock()
	defer px.mu.Unlock()
	if maxDone > px.maxDone[server] {
		px.maxDone[server] = maxDone
	}
	return px.maxDone[px.me]
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	inst := px.findOrCreate(args.Seq, nil, Serving)
	reply.MaxDone = px.updateMaxDone(args.Caller, args.MaxDone)
	inst.mu.Lock()
	defer inst.mu.Unlock()
	prefix := fmt.Sprintf("prepare %d->%d with %v on status %d", args.Caller, px.me, args, inst.status)
	if args.N > inst.np {
		inst.np = args.N
		reply.Err = OK
		reply.NA = inst.na
		reply.VA = inst.va
		DPrintf("%s ok", prefix)
		return nil
	}
	DPrintf("%s rejected %d vs %d", prefix, args.N, inst.np)
	reply.Err = ErrReject
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	inst := px.findOrCreate(args.Seq, nil, Serving)
	reply.MaxDone = px.updateMaxDone(args.Caller, args.MaxDone)
	inst.mu.Lock()
	defer inst.mu.Unlock()
	prefix := fmt.Sprintf("accept %d->%d with %d, %d on status %d", args.Caller, px.me, args.Seq, args.N, inst.status)
	if args.N >= inst.np {
		inst.np = args.N
		inst.na = args.N
		DPrintf("%s accept value %d: %v", prefix, args.Seq, args.V)
		inst.va = args.V
		reply.Err = OK
		DPrintf("%s ok", prefix)
		return nil
	}
	DPrintf("%s rejected %d vs %d", prefix, args.N, inst.np)
	reply.Err = ErrReject
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	inst := px.findOrCreate(args.Seq, nil, Serving)
	reply.MaxDone = px.updateMaxDone(args.Caller, args.MaxDone)
	inst.mu.Lock()
	defer inst.mu.Unlock()
	prefix := fmt.Sprintf("decide %d->%d with %v on status %d", args.Caller, px.me, args.Seq, inst.status)
	inst.status = Decided
	DPrintf("%s decide value %d: %v", prefix, args.Seq, args.V)
	inst.va = args.V
	DPrintf("%s ok", prefix)
	reply.Err = OK
	return nil
}
