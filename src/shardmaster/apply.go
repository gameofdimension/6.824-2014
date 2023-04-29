package shardmaster

import (
	"fmt"
	"log"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (sm *ShardMaster) isJoinArgs(args interface{}) bool {
	switch args.(type) {
	case JoinArgs:
		return true
	default:
		return false
	}
}

func (sm *ShardMaster) sameOp(op1 *Op, op2 *Op) bool {
	if op1.Type != op2.Type {
		return false
	}
	if sm.isJoinArgs(op1.Args) && sm.isJoinArgs(op2.Args) {
		j1 := op1.Args.(JoinArgs)
		j2 := op2.Args.(JoinArgs)
		if j1.GID != j2.GID {
			return false
		}
		if len(j1.Servers) != len(j2.Servers) {
			return false
		}
		for i := 0; i < len(j1.Servers); i += 1 {
			if j1.Servers[i] != j2.Servers[i] {
				return false
			}
		}
		return true
	}
	return op1.Args == op2.Args
}

func (sm *ShardMaster) pollPaxos(instance int, targetOp *Op) (bool, *Op) {
	to := 10 * time.Millisecond
	for !sm.dead {
		decided, value := sm.px.Status(instance)
		copy := *targetOp
		copy.Args = "*masked*"
		DPrintf("me %d poll status of %d expected %v", sm.me, instance, copy)
		if decided {
			agreement := value.(Op)
			if sm.sameOp(&agreement, targetOp) {
				return true, &agreement
			} else {
				return false, &agreement
			}
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return false, nil
}

func (sm *ShardMaster) fillGap(seq int) {
	op := Op{
		Type: OpNoop,
	}
	sm.px.Start(seq, op)
	DPrintf("me %d fill gap seq %d with noop started", sm.me, seq)
	ok, actual := sm.pollPaxos(seq, &op)
	copy := actual
	if copy != nil {
		copy.Args = "*masked*"
	}
	DPrintf("me %d fill gap seq %d with noop result %t, %v", sm.me, seq, ok, copy)
}

func (sm *ShardMaster) appleyLog() {
	for !sm.dead {
		max := sm.px.Max()
		for i := sm.lastApplied + 1; i <= max; i += 1 {
			prefix := fmt.Sprintf("apply log me %d apply range [%d vs %d] at %d", sm.me, sm.lastApplied, max, i)
			DPrintf("%s started", prefix)
			decided, val := sm.px.Status(i)
			if !decided {
				if i < max {
					DPrintf("%s not decided try fill gap", prefix)
					sm.fillGap(i)
					decided, val = sm.px.Status(i)
					if !decided {
						panic(fmt.Sprintf("seq %d not decided after fill gap", i))
					}
				} else {
					// i==max 的情况下提前返回非常重要，因为要补的是中间的洞，而处在日志尾巴的洞不能认为是洞，而可能是
					// 正在形成共识的新操作
					time.Sleep(1 * time.Millisecond)
					continue
				}
			}
			sm.mu.Lock()
			op := val.(Op)
			if op.Type == OpQuery {
				args := op.Args.(QueryArgs)
				version := args.Num
				if version == -1 || version >= len(sm.configs) {
					version = len(sm.configs) - 1
				}
			} else if op.Type == OpJoin {
				args := op.Args.(JoinArgs)
				config := sm.makeConfigForJoin(&args)
				DPrintf("server %d latest config after join %v", sm.me, config)
				sm.configs = append(sm.configs, *config)
			} else if op.Type == OpLeave {
				args := op.Args.(LeaveArgs)
				config := sm.makeConfigForLeave(&args)
				DPrintf("server %d latest config after leave %v", sm.me, config)
				sm.configs = append(sm.configs, *config)
			} else if op.Type == OpMove {
				args := op.Args.(MoveArgs)
				config := sm.makeConfigForMove(&args)
				DPrintf("server %d latest config after move %v", sm.me, config)
				sm.configs = append(sm.configs, *config)
			}
			sm.lastApplied = i
			minPending := sm.minPendingInstance()
			sm.mu.Unlock()
			if i < minPending {
				sm.px.Done(i)
				min := sm.px.Min()
				DPrintf("%s done %d min %d", prefix, i, min)
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (sc *ShardMaster) makeConfigForJoin(args *JoinArgs) *Config {
	version := len(sc.configs)
	groups := make(map[int64][]string)
	last := sc.configs[version-1]
	for k, v := range last.Groups {
		groups[k] = v
	}
	groups[args.GID] = args.Servers

	finalGids := []int64{}
	for k := range groups {
		finalGids = append(finalGids, int64(k))
	}

	shards := add(last.Shards[:], finalGids)
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	return &config
}

func (sc *ShardMaster) makeConfigForLeave(args *LeaveArgs) *Config {
	version := len(sc.configs)
	groups := make(map[int64][]string)
	last := sc.configs[version-1]
	finalGids := make([]int64, 0)
	for k, v := range last.Groups {
		if k != args.GID {
			groups[int64(k)] = v
			finalGids = append(finalGids, int64(k))
		}
	}
	shards := remove(last.Shards[:], finalGids)
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	return &config
}

func (sc *ShardMaster) makeConfigForMove(args *MoveArgs) *Config {
	version := len(sc.configs)
	groups := make(map[int64][]string)
	for k, v := range sc.configs[version-1].Groups {
		groups[k] = v
	}
	shards := sc.configs[version-1].Shards[:]
	oldGid := shards[args.Shard]
	cc := 0
	for _, v := range shards {
		if v == oldGid {
			cc += 1
		}
	}
	if cc == 1 {
		delete(groups, oldGid)
	}

	shards[args.Shard] = args.GID
	config := Config{
		Num:    version,
		Groups: groups,
		Shards: SliceToArr(shards),
	}
	return &config
}
