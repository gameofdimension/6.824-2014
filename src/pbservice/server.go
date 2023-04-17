package pbservice

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
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
	lastDumpView     uint
}

func (pb *PBServer) isPrimary() bool {
	return pb.me == pb.primary
}

func (pb *PBServer) forwardToBackup(me string, viewnum uint, primary string, backup string, args *SyncArgs) bool {
	if backup == "" {
		return true
	}
	for !pb.killed() {
		reply := SyncReply{}
		prefix := fmt.Sprintf("forwardToBackup %v is primary? %t forward to backup %s with %v", me, me == primary, backup, *args)
		DPrintf("%s", prefix)
		ret := call(backup, "PBServer.Forward", args, &reply)
		if ret && reply.Err == ErrWrongView {
			DPrintf("%s fail with view not match %v", prefix, reply)
			return false
		}
		if ret && reply.Err == OK {
			DPrintf("%s sync done", prefix)
			break
		}
		view, rc := pb.vs.Get()
		if rc && view.Backup != backup {
			DPrintf("%s view changed %v vs [%d, %s, %s]", prefix, view, viewnum, primary, backup)
			break
		}
		DPrintf("%s will retry", prefix)
		time.Sleep(viewservice.PingInterval)
	}
	return true
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	// 该函数不应该阻塞，否则会造成 view service 将其下线
	prefix := fmt.Sprintf("server tick me %v is primary? %t ping view %d", pb.me, pb.isPrimary(), pb.viewNum)
	view, err := pb.vs.Ping(pb.viewNum)
	if err == nil {
		if view.Viewnum < pb.viewNum {
			panic(fmt.Sprintf("view num get smaller %d vs %d impossible", view.Viewnum, pb.viewNum))
		} else if view.Viewnum > pb.viewNum {
			go func(expected uint) {
				if view.Primary == pb.me && view.Backup != "" {
					DPrintf("%s will dump state to %s", prefix, view.Backup)
					pb.mu.Lock()
					args := DumpArgs{
						Viewnum:          view.Viewnum,
						LastClientSeq:    pb.lastClientSeq,
						LastClientResult: pb.lastClientResult,
						Repo:             pb.repo,
					}
					pb.mu.Unlock()
					if !pb.dumpToBackup(view.Viewnum, view.Backup, &args) {
						DPrintf("%s fail dump state to %s", prefix, view.Backup)
						return
					}
				}
				DPrintf("%s will update to view %v", prefix, view)
				pb.mu.Lock()
				if expected == pb.viewNum {
					pb.viewNum = view.Viewnum
					pb.primary = view.Primary
					pb.backup = view.Backup
				}
				pb.mu.Unlock()
			}(pb.viewNum)
		} else {
			DPrintf("%s view not change", prefix)
		}
	} else {
		DPrintf("%s ping view err %v, %v", prefix, err, view)
	}
}

func (pb *PBServer) dumpToBackup(viewnum uint, backup string, args *DumpArgs) bool {
	prefix := fmt.Sprintf("dumpToBackup me %v is primary? %t to %s with %v", pb.me, pb.isPrimary(), backup, *args)
	for !pb.killed() {
		reply := DumpReply{}
		ret := call(backup, "PBServer.Dump", args, &reply)
		if ret && (reply.Err == OK || reply.Err == ErrObsoleteView) {
			DPrintf("%s dump ok with %v", prefix, reply)
			return true
		}
		if ret && reply.Err == ErrWrongView {
			break
		}
		view, rc := pb.vs.Get()
		if rc && viewnum != view.Viewnum {
			DPrintf("%s view changed %v vs [%d, %s, %s]", prefix, view, pb.viewNum, pb.primary, pb.backup)
			return true
		}
		DPrintf("%s will retry", prefix)
		time.Sleep(viewservice.PingInterval)
	}
	DPrintf("%s dump fail", prefix)
	return false
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	DPrintf("kill %s, %t", pb.me, pb.isPrimary())
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func (pb *PBServer) killed() bool {
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
