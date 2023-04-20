package kvpaxos

import (
	"fmt"
	"strconv"
)

func (kv *KVPaxos) appleyLog() {
	for !kv.dead {
		for i := kv.lastApply + 1; i <= kv.lastAgree; i += 1 {
			prefix := fmt.Sprintf("apply log me %d apply range [%d vs %d] at %d", kv.me, kv.lastApply, kv.lastAgree, i)
			DPrintf("%s started", prefix)
			decided, val := kv.px.Status(i)
			if !decided {
				DPrintf("%s not decided try fill gap", prefix)
				kv.fillGap(i)
				decided, val = kv.px.Status(i)
				if !decided {
					panic(fmt.Sprintf("seq %d not decided after fill gap", i))
				}
			}
			kv.mu.Lock()
			op := val.(Op)
			lastSeq, ok := kv.lastClientSeq[op.ClientId]
			if !ok || lastSeq != op.ClientSeq {
				if op.ClientId < lastSeq {
					panic(fmt.Sprintf("smaller seq %d vs %d for %d", op.ClientId, lastSeq, op.ClientId))
				}
				if op.Type == OpPut {
					key := op.Key
					value := op.Value
					previous := kv.repo[key]
					kv.lastClientSeq[op.ClientId] = op.ClientSeq
					if op.DoHash {
						h := hash(previous + value)
						value = strconv.Itoa(int(h))
						kv.lastClientResult[op.ClientId] = previous
					} else {
						kv.lastClientResult[op.ClientId] = true
					}
					DPrintf("%s put log of key %s: %s->%s", prefix, key, kv.repo[key], value)
					kv.repo[key] = value
				} else if op.Type == OpGet {
					key := op.Key
					kv.lastClientSeq[op.ClientId] = op.ClientSeq
					if value, ok := kv.repo[key]; ok {
						kv.lastClientResult[op.ClientId] = value
					} else {
						kv.lastClientResult[op.ClientId] = nil
					}
					DPrintf("%s get log of key %s: %s", prefix, key, kv.repo[key])
				}
			}
			kv.lastApply = i
			kv.mu.Unlock()
			kv.px.Done(i)
			min := kv.px.Min()
			DPrintf("%s done %d min %d", prefix, i, min)
		}
	}
}
