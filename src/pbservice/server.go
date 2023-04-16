package pbservice

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"6.824.2014/viewservice"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       int32 // for testing
	unreliable bool  // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	mu sync.Mutex

	viewNum          uint
	primary          string
	backup           string
	lastClientSeq    map[int64]int64
	lastClientResult map[int64]interface{}
	repo             map[string]string
}

func (pb *PBServer) isPrimary() bool {
	return pb.me == pb.primary
}

func (pb *PBServer) forwardToBackup(args *SyncArgs) bool {
	if pb.backup == "" {
		return true
	}
	reply := SyncReply{}
	prefix := fmt.Sprintf("syncToBackup %v is primary %t sync to backup %s with %v", pb.me, pb.isPrimary(), pb.backup, *args)
	DPrintf("%s", prefix)
	ret := call(pb.backup, "PBServer.Forward", args, &reply)
	DPrintf("%s, return with %t, %v", prefix, ret, reply)
	if ret && reply.Err == ErrWrongView {
		return false
	}
	return true
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
			panic(fmt.Sprintf("client %d seq out of order %d vs %d", client, seq, lastSeq))
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
			panic(fmt.Sprintf("client %d seq out of order %d vs %d", client, seq, lastSeq))
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
				panic(fmt.Sprintf("client %d seq out of order %d vs %d", client, seq, lastSeq))
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

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	prefix := fmt.Sprintf("server tick me %v is primary %t ping view %d", pb.me, pb.isPrimary(), pb.viewNum)
	view, err := pb.vs.Ping(pb.viewNum)
	if err == nil {
		if view.Primary == pb.me {
			if view.Backup != "" && view.Backup != pb.backup {
				DPrintf("%s will dump state to %s", prefix, view.Backup)
				args := DumpArgs{
					Viewnum:          view.Viewnum,
					LastClientSeq:    pb.lastClientSeq,
					LastClientResult: pb.lastClientResult,
					Repo:             pb.repo,
				}
				if !pb.dumpToBackup(view.Backup, &args) {
					DPrintf("%s fail dump state to %s", prefix, view.Backup)
					return
				}
			}
		}
		DPrintf("%s will update to view %v", prefix, view)
		pb.viewNum = view.Viewnum
		pb.primary = view.Primary
		pb.backup = view.Backup
	} else {
		DPrintf("%s ping view return %v, %v", prefix, view, err)
	}
}

func (pb *PBServer) dumpToBackup(backup string, args *DumpArgs) bool {
	reply := DumpReply{}
	prefix := fmt.Sprintf("dumpToBackup me %v is primary %t to %s with %v", pb.me, pb.isPrimary(), backup, *args)
	ret := call(backup, "PBServer.Dump", args, &reply)
	if ret && reply.Err == OK {
		DPrintf("%s dump ok", prefix)
		return true
	}
	DPrintf("%s dump fail with %v, %v", prefix, ret, reply)
	return false
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

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// pb.dead = true
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func (pb *PBServer) killed() bool {
	// pb.mu.Lock()
	// defer pb.mu.Unlock()
	// return pb.dead
	z := atomic.LoadInt32(&pb.dead)
	return z == 1
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	pb.repo = make(map[string]string)
	pb.lastClientSeq = make(map[int64]int64)
	pb.lastClientResult = make(map[int64]interface{})
	gob.Register(PutArgs{})
	gob.Register(GetArgs{})

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for !pb.killed() {
			conn, err := pb.l.Accept()
			if err == nil && !pb.killed() {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && !pb.killed() {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for !pb.killed() {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
