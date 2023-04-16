package pbservice

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"time"

	"6.824.2014/viewservice"
)

// You'll probably need to uncomment these:
// import "time"
// import "crypto/rand"
// import "math/big"

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here

	me      int64
	seq     int64
	viewNum uint
	primary string
	backup  string
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here

	ck.me = nrand()
	ck.seq = 0
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
func (ck *Clerk) Get(key string) string {
	// Your code here.
	ck.seq += 1
	args := GetArgs{
		Key: key,
		Id:  ck.me,
		Seq: ck.seq,
	}
	reply := GetReply{}
	for {
		DPrintf("get %v, %v\n", ck.primary, ck.me)
		if ck.primary != "" {
			for {
				DPrintf("call rpc get %v, %v\n", ck.primary, ck.me)
				ret := call(ck.primary, "PBServer.Get", &args, &reply)
				DPrintf("after call rpc get %v, %v, %v\n", ck.primary, ck.me, ret)
				if !ret {
					break
				}
				if reply.Err == OK {
					return reply.Value
				}
				if reply.Err == ErrNoKey {
					return ""
				}
				if reply.Err == ErrWrongServer {
					break
				}
			}
		}
		view, err := ck.vs.Get()
		if !err {
			DPrintf("get view error %v\n", err)
		} else {
			ck.viewNum = view.Viewnum
			ck.primary = view.Primary
			ck.backup = view.Backup
		}
		time.Sleep(viewservice.PingInterval)
	}
}

// tell the primary to update key's value.
// must keep trying until it succeeds.
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// Your code here.
	ck.seq += 1
	args := PutArgs{
		Key:    key,
		Value:  value,
		DoHash: dohash,
		Id:     ck.me,
		Seq:    ck.seq,
	}
	reply := PutReply{}
	for {
		DPrintf("putext %v, %v\n", ck.primary, ck.me)
		if ck.primary != "" {
			for {
				DPrintf("call rpc %v, %v\n", ck.primary, ck.me)
				ret := call(ck.primary, "PBServer.Put", &args, &reply)
				DPrintf("after call rpc %v, %v\n", ck.primary, ck.me)
				if !ret {
					break
				}
				if reply.Err == OK {
					return reply.PreviousValue
				}
				if reply.Err == ErrWrongServer {
					break
				}
			}
		}
		view, err := ck.vs.Get()
		if !err {
			DPrintf("get view error %v\n", err)
		} else {
			DPrintf("PutExt get new view: %v\n", view)
			ck.viewNum = view.Viewnum
			ck.primary = view.Primary
			ck.backup = view.Backup
		}
		time.Sleep(viewservice.PingInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
