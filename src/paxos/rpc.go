package paxos

const (
	OK        = "OK"
	ErrReject = "ErrReject"
)

type Err string

type PrepareArgs struct {
	Caller int
	Seq    int
	N      int
}

type PrepareReply struct {
	Err Err
	NA  int
	VA  interface{}
}

type AcceptArgs struct {
	Caller int
	Seq    int
	N      int
	V      interface{}
}

type AcceptReply struct {
	Err Err
}

type DecideArgs struct {
	Caller int
	Seq    int
	V      interface{}
}

type DecideReply struct {
	Err Err
}

func (px *Paxos) findOrCreate(seq int, status Status) *Instance {
	px.mu.Lock()
	defer px.mu.Unlock()
	inst, ok := px.seqToInstance[seq]
	if !ok {
		tmp := MakeInstance(seq, nil, len(px.peers), px.me, status)
		inst = &tmp
		px.seqToInstance[seq] = inst
	}
	return inst
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	inst := px.findOrCreate(args.Seq, Serving)
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if args.N > inst.np {
		inst.np = args.N
		reply.Err = OK
		return nil
	}
	reply.Err = ErrReject
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	inst := px.findOrCreate(args.Seq, Serving)
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if args.N >= inst.np {
		inst.np = args.N
		inst.na = args.N
		inst.va = args.V
		reply.Err = OK
		return nil
	}
	reply.Err = ErrReject
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	inst := px.findOrCreate(args.Seq, Serving)
	inst.mu.Lock()
	defer inst.mu.Unlock()
	inst.status = Decided
	inst.va = args.V
	reply.Err = OK
	return nil
}
