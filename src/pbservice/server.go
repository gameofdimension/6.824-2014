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

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
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
	// pb.mu.Lock()
	// defer pb.mu.Unlock()
	// DPrintf("xxxxxxxxxxxxxxxxxxxx %v\n", pb.backup)
	if pb.backup == "" {
		return true
	}
	// DPrintf("yyyyyyyyyyyyyyyyyyy\n")
	for !pb.killed() {
		// DPrintf("zzzzzzzzzzzzzzzzz\n")
		reply := SyncReply{}
		DPrintf("111111111 syncToBackup %v, %v, %v\n", pb.backup, pb.me, *args)
		ret := call(pb.backup, "PBServer.Forward", args, &reply)
		DPrintf("111111111 after syncToBackup %v, %v, %v\n", pb.backup, pb.me, ret)
		if ret {
			if reply.Err == OK {
				return true
			}
			if reply.Err == ErrWrongView {
				break
			}
		}
	}
	return false
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	DPrintf("put handler %v, %v, %v\n", *args, pb.me, pb.primary)
	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}
	DPrintf("11111 put handler %v, %v, %v\n", *args, pb.me, pb.primary)
	req := SyncArgs{
		Op:      SyncTypePut,
		Args:    *args,
		Viewnum: pb.viewNum,
	}
	DPrintf("22222 put handler %v, %v, %v\n", *args, pb.me, pb.primary)
	if !pb.forwardToBackup(&req) {
		reply.Err = ErrWrongView
		return nil
	}
	DPrintf("33333 put handler %v, %v, %v\n", *args, pb.me, pb.primary)
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
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	DPrintf("get handler %v, %v\n", pb.primary, pb.me)
	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}
	req := SyncArgs{
		Op:      SyncTypeGet,
		Args:    *args,
		Viewnum: pb.viewNum,
	}
	if !pb.forwardToBackup(&req) {
		reply.Err = ErrWrongView
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
	return nil
}

func (pb *PBServer) Forward(args *SyncArgs, reply *SyncReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if args.Viewnum < pb.viewNum {
		reply.Err = ErrWrongView
		return nil
	}
	if args.Viewnum > pb.viewNum {
		reply.Err = ErrFutureView
		return nil
	}
	if pb.backup != pb.me {
		panic(fmt.Sprintf("same view %d, different perception [%s, %s]", pb.viewNum, pb.primary, pb.backup))
	}
	DPrintf("forward %v, me: %v\n", args, pb.me)
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
		return nil
	}
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	view, err := pb.vs.Ping(pb.viewNum)
	prefix := fmt.Sprintf("tick me: %v, is primary: %t, view %d", pb.me, pb.isPrimary(), pb.viewNum)
	DPrintf("%s ping return %v, %v\n", prefix, view, err)
	if err == nil {
		// DPrintf("server tick get new view: %v vs [%d, %s, %s]\n", view, pb.viewNum, pb.primary, pb.backup)
		if view.Primary == pb.me {
			if view.Backup != "" && view.Backup != pb.backup {
				DPrintf("%s will dump state to %s\n", prefix, view.Backup)
				args := DumpArgs{
					Viewnum:          view.Viewnum,
					LastClientSeq:    pb.lastClientSeq,
					LastClientResult: pb.lastClientResult,
					Repo:             pb.repo,
				}
				if !pb.dumpToBackup(view.Backup, &args) {
					return
				}
			}
		}
		pb.viewNum = view.Viewnum
		pb.primary = view.Primary
		pb.backup = view.Backup
	}
}

func (pb *PBServer) dumpToBackup(backup string, args *DumpArgs) bool {
	reply := DumpReply{}
	prefix := fmt.Sprintf("dumpToBackup me: %v, is primary: %t, view %d", pb.me, pb.isPrimary(), pb.viewNum)
	ret := call(backup, "PBServer.Dump", args, &reply)
	DPrintf("%s dump result %v, %v\n", prefix, ret, reply)
	if ret && reply.Err == OK {
		return true
	}
	return false
}

func (pb *PBServer) Dump(args *DumpArgs, reply *DumpReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if args.Viewnum < pb.viewNum {
		reply.Err = ErrWrongView
		return nil
	}
	if args.Viewnum > pb.viewNum {
		reply.Err = ErrFutureView
		return nil
	}
	if pb.backup != pb.me {
		panic(fmt.Sprintf("same view %d, different perception [%s, %s]", pb.viewNum, pb.backup, pb.me))
	}

	reply.Err = OK
	pb.repo = args.Repo
	pb.lastClientSeq = args.LastClientSeq
	pb.lastClientResult = args.LastClientResult
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
		DPrintf("%s: wait until all request are done\n", pb.me)
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
