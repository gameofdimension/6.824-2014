package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	num      uint
	acked    bool
	primary  string
	backup   string
	lastPing map[string]time.Time
	lastNum  map[string]uint
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.lastPing[args.Me] = time.Now()
	vs.lastNum[args.Me] = args.Viewnum
	if vs.num == 0 {
		vs.num = 1
		vs.acked = false
		vs.primary = args.Me

		reply.View.Viewnum = uint(vs.num)
		reply.View.Primary = vs.primary
		return nil
	}
	if args.Me == vs.primary && args.Viewnum == uint(vs.num) && !vs.acked {
		vs.acked = true
	}
	reply.View.Viewnum = uint(vs.num)
	reply.View.Primary = vs.primary
	reply.View.Backup = vs.backup
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.num == 0 {
		reply.View.Viewnum = 0
		return nil
	}
	reply.View.Viewnum = uint(vs.num)
	reply.View.Primary = vs.primary
	reply.View.Backup = vs.backup
	return nil
}

func (vs *ViewServer) isDead(server string) bool {
	if time.Since(vs.lastPing[server]) > DeadPings*PingInterval {
		return true
	}
	return false
}

func (vs *ViewServer) isRestarted(server string) bool {
	return !vs.isDead(server) && vs.lastNum[server] == 0
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.num == 0 {
		return
	}
	if !vs.acked {
		return
	}
	next := vs.chooseOne()
	if vs.isDead(vs.primary) || vs.isRestarted(vs.primary) {
		vs.acked = false
		vs.num += 1
		vs.primary = vs.backup
		vs.backup = ""
		if next != "" {
			vs.backup = next
		}
	} else if vs.backup == "" {
		if next != "" {
			vs.acked = false
			vs.num += 1
			vs.backup = next
		}
	} else if vs.isDead(vs.backup) || vs.isRestarted(vs.backup) {
		vs.acked = false
		vs.num += 1
		vs.backup = next
	}
}

func (vs *ViewServer) chooseOne() string {
	for k, v := range vs.lastPing {
		if k == vs.primary || k == vs.backup {
			continue
		}
		if time.Since(v) <= PingInterval {
			return k
		}
	}
	return ""
}

// tell the server to shut itself down.
// for testing.
// please don't change this function.
func (vs *ViewServer) Kill() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.dead = true
	vs.l.Close()
}

func (vs *ViewServer) Killed() bool {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.dead
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.lastPing = make(map[string]time.Time)
	vs.lastNum = make(map[string]uint)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for !vs.Killed() {
			conn, err := vs.l.Accept()
			if err == nil && !vs.Killed() {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && !vs.Killed() {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for !vs.Killed() {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
