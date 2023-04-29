package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"

	"6.824.2014/paxos"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs         []Config // indexed by config num
	lastApplied     int
	pendingInstance map[int]bool
}

type OpType int

const (
	OpNoop  = 0
	OpQuery = 1
	OpJoin  = 2
	OpLeave = 3
	OpMove  = 4
)

type Op struct {
	// Your data here.
	Type OpType
	Args interface{}
}

func (sm *ShardMaster) minPendingInstance() int {
	res := math.MaxInt32
	for k := range sm.pendingInstance {
		if k < res {
			res = k
		}
	}
	return res
}

func (sm *ShardMaster) clearPendingInstance(instance int) {
	DPrintf("server %d clear instance %d", sm.me, instance)
	sm.mu.Lock()
	delete(sm.pendingInstance, instance)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	prefix := fmt.Sprintf("server %d join %v", sm.me, args)
	DPrintf("%s start", prefix)

	op := Op{
		Type: OpJoin,
		Args: *args,
	}
	sm.mu.Lock()
	instance := sm.px.Max() + 1
	sm.pendingInstance[instance] = true
	sm.mu.Unlock()
	defer sm.clearPendingInstance(instance)
	sm.px.Start(instance, op)
	DPrintf("%s start paxos %d", prefix, instance)
	ok, actual := sm.pollPaxos(instance, &op)
	DPrintf("%s return %t, %v vs %v", prefix, ok, op, *actual)
	if !ok {
		return fmt.Errorf("%s agreement fail", prefix)
	}
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	prefix := fmt.Sprintf("server %d leave %v", sm.me, args)
	DPrintf("%s start", prefix)

	op := Op{
		Type: OpLeave,
		Args: *args,
	}
	sm.mu.Lock()
	instance := sm.px.Max() + 1
	sm.pendingInstance[instance] = true
	sm.mu.Unlock()
	defer sm.clearPendingInstance(instance)
	sm.px.Start(instance, op)
	DPrintf("%s start paxos %d", prefix, instance)
	ok, actual := sm.pollPaxos(instance, &op)
	DPrintf("%s return %t, %v vs %v", prefix, ok, op, *actual)
	if !ok {
		return fmt.Errorf("%s agreement fail", prefix)
	}
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	prefix := fmt.Sprintf("server %d move %v", sm.me, args)
	DPrintf("%s start", prefix)

	op := Op{
		Type: OpMove,
		Args: *args,
	}
	sm.mu.Lock()
	instance := sm.px.Max() + 1
	sm.pendingInstance[instance] = true
	sm.mu.Unlock()
	defer sm.clearPendingInstance(instance)
	sm.px.Start(instance, op)
	DPrintf("%s start paxos %d", prefix, instance)
	ok, actual := sm.pollPaxos(instance, &op)
	DPrintf("%s return %t, %v vs %v", prefix, ok, op, *actual)
	if !ok {
		return fmt.Errorf("%s agreement fail", prefix)
	}
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	prefix := fmt.Sprintf("server %d query %v", sm.me, args)
	DPrintf("%s start", prefix)

	op := Op{
		Type: OpQuery,
		Args: *args,
	}
	sm.mu.Lock()
	instance := sm.px.Max() + 1
	sm.pendingInstance[instance] = true
	sm.mu.Unlock()
	defer sm.clearPendingInstance(instance)
	sm.px.Start(instance, op)
	DPrintf("%s start paxos %d", prefix, instance)
	ok, actual := sm.pollPaxos(instance, &op)
	DPrintf("%s return %t, %v vs %v", prefix, ok, op, *actual)
	if !ok {
		return fmt.Errorf("%s agreement fail", prefix)
	}
	for !sm.dead {
		if sm.lastApplied < instance {
			time.Sleep(3 * time.Millisecond)
			continue
		}
		sm.mu.Lock()
		version := args.Num
		if version < 0 || version >= len(sm.configs) {
			version = len(sm.configs) - 1
		}
		reply.Config = sm.configs[version]
		sm.mu.Unlock()
		break
	}
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(JoinReply{})
	gob.Register(LeaveArgs{})
	gob.Register(LeaveReply{})
	gob.Register(MoveArgs{})
	gob.Register(MoveReply{})
	gob.Register(QueryArgs{})
	gob.Register(QueryReply{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	sm.lastApplied = -1
	sm.pendingInstance = make(map[int]bool)
	go sm.appleyLog()

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
