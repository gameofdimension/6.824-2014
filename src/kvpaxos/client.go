package kvpaxos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"time"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	servers []string
	// You will have to modify this struct.

	me  int64
	seq int64
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
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
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		DPrintf("rpc dial error %v", errx)
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	DPrintf("rpc call error %v", err)
	return false
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seq += 1
	args := GetArgs{
		Key: key,
		Id:  ck.me,
		Seq: ck.seq,
	}
	for {
		for idx, server := range ck.servers {
			reply := GetReply{}
			prefix := fmt.Sprintf("get client %d call server %d with %v", ck.me, idx, args)
			ok := call(server, "KVPaxos.Get", &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("%s ok with value %s", prefix, reply.Value)
				return reply.Value
			}
			if ok && reply.Err == ErrNoKey {
				DPrintf("%s no key", prefix)
				return ""
			}
			DPrintf("%s fail %t, %v", prefix, ok, reply)
		}
		time.Sleep(7 * time.Millisecond)
	}
}

// set the value for a key.
// keeps trying until it succeeds.
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// You will have to modify this function.
	ck.seq += 1
	args := PutArgs{
		Key:    key,
		Value:  value,
		DoHash: dohash,
		Id:     ck.me,
		Seq:    ck.seq,
	}
	for {
		for idx, server := range ck.servers {
			reply := PutReply{}
			prefix := fmt.Sprintf("put client %d call server %d with %v", ck.me, idx, args)
			ok := call(server, "KVPaxos.Put", &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("%s ok", prefix)
				return reply.PreviousValue
			}
			DPrintf("%s fail %t, %v", prefix, ok, reply)
		}
		time.Sleep(7 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
