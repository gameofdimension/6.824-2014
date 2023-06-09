package paxos

import (
	"log"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Status int

const (
	Accepted = 0
	Serving  = 1
	Running  = 2
	Decided  = 3
)

type Instance struct {
	seq    int
	value  interface{}
	status Status
	mu     sync.Mutex

	peerNum     int
	proposalNum int
	np          int
	na          int
	va          interface{}
}

func MakeInstance(seq int, value interface{}, peerNum int, firtPropose int, status Status) Instance {
	return Instance{
		seq:         seq,
		value:       value,
		status:      status,
		peerNum:     peerNum,
		proposalNum: firtPropose,
		np:          -1,
		na:          -1,
		va:          nil,
	}
}

func nextProposalNum(init int, step int, np int) int {
	for {
		init += step
		if init > np {
			return init
		}
	}
}

func (px *Paxos) doPrepare(peers []string, proposalNum int, inst *Instance) (bool, int, interface{}) {
	seq := inst.seq
	count := 0
	maxNa := -1
	var maxNaV interface{}
	for idx, peer := range peers {
		if idx == px.me {
			inst.mu.Lock()
			if proposalNum <= inst.np {
				DPrintf("doPrepare impossible proposal num %d vs %d", proposalNum, inst.np)
			} else {
				// prepare ok for self
				inst.np = proposalNum
				count += 1
				if inst.na > maxNa {
					DPrintf("doPrepare me %d self bigger na: %d vs %d", px.me, inst.na, maxNa)
					maxNa = inst.na
					maxNaV = inst.va
				}
			}
			inst.mu.Unlock()
		} else {
			px.mu.Lock()
			args := PrepareArgs{
				MaxDone: px.maxDone[px.me],
				Caller:  px.me,
				Seq:     seq,
				N:       proposalNum,
			}
			px.mu.Unlock()
			reply := PrepareReply{}
			ok := call(peer, "Paxos.Prepare", &args, &reply)
			if ok && reply.Err == OK {
				px.updateMaxDone(idx, reply.MaxDone)
				count += 1
				if reply.NA > maxNa {
					DPrintf("doPrepare me %d from %d bigger na: %d vs %d", px.me, idx, reply.NA, maxNa)
					maxNa = reply.NA
					maxNaV = reply.VA
				}
			}
		}
	}
	if count*2 > len(peers) {
		return true, maxNa, maxNaV
	}
	return false, maxNa, maxNaV
}

func (px *Paxos) doAccept(peers []string, proposalNum int, value interface{}, inst *Instance) bool {
	seq := inst.seq
	count := 0
	for idx, peer := range peers {
		if idx == px.me {
			inst.mu.Lock()
			if proposalNum < inst.np {
				DPrintf("doAccept impossible proposal num %d vs %d", proposalNum, inst.np)
			} else {
				// accept ok for self
				inst.np = proposalNum
				inst.na = proposalNum
				DPrintf("doAccept me %d self accept value at seq %d", px.me, seq)
				inst.va = value
				count += 1
			}
			inst.mu.Unlock()
		} else {
			px.mu.Lock()
			args := AcceptArgs{
				MaxDone: px.maxDone[px.me],
				Caller:  px.me,
				Seq:     seq,
				N:       proposalNum,
				V:       value,
			}
			px.mu.Unlock()
			reply := AcceptReply{}
			ok := call(peer, "Paxos.Accept", &args, &reply)
			if ok && reply.Err == OK {
				px.updateMaxDone(idx, reply.MaxDone)
				count += 1
			}
		}
	}
	return count*2 > len(peers)
}

func (px *Paxos) doDecide(peers []string, value interface{}, inst *Instance) int {
	seq := inst.seq
	count := 0
	for idx, peer := range peers {
		if idx == px.me {
			inst.mu.Lock()
			inst.status = Decided
			inst.va = value
			inst.mu.Unlock()
			count += 1
			continue
		}
		px.mu.Lock()
		args := DecideArgs{
			MaxDone: px.maxDone[px.me],
			Caller:  px.me,
			Seq:     seq,
			V:       value,
		}
		px.mu.Unlock()
		reply := DecideReply{}
		ok := call(peer, "Paxos.Decide", &args, &reply)
		if ok && reply.Err == OK {
			px.updateMaxDone(idx, reply.MaxDone)
			count += 1
		}
	}
	return count
}

func (px *Paxos) run(peers []string, inst *Instance) {
	for !px.dead {
		inst.proposalNum = nextProposalNum(inst.proposalNum, len(peers), inst.np)
		proposalNum := inst.proposalNum
		ok, na, va := px.doPrepare(peers, proposalNum, inst)
		if !ok {
			DPrintf("prepare fail me %d seq %d np %d", px.me, inst.seq, proposalNum)
			continue
		}

		value := inst.value
		if na >= 0 && va != nil {
			value = va
		}
		rc := px.doAccept(peers, proposalNum, value, inst)
		if !rc {
			DPrintf("accept fail me %d seq %d np %d", px.me, inst.seq, proposalNum)
			continue
		}

		ret := px.doDecide(peers, value, inst)
		DPrintf("decided ok on %d peers", ret)
		break
	}
}
