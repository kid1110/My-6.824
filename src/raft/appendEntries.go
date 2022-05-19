package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool

	XLen   int
	XTerm  int
	XIndex int
}

func (rf *Raft) appendEntries(heartbeat bool) {
	lastLog := rf.log.lastLog()

	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}
		nextIndex := rf.nextIndex[peer]
		if lastLog.Index >= nextIndex || heartbeat {
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}
			prevLog := rf.log.at(nextIndex - 1)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      make([]Entry, lastLog.Index-nextIndex+1),
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log.slice(nextIndex))
			go rf.leaderSendEntries(peer, &args)
		}
	}
}
func (rf *Raft) leaderSendEntries(peer int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, &reply)
	if !ok {
		DPrintf("[server %v] can not send appendEntries to other servers", rf.me)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if rf.currentTerm == args.Term {
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[peer] = max(rf.nextIndex[peer], next)
			rf.matchIndex[peer] = max(rf.matchIndex[peer], match)
			DPrintf("[server %v]: %v append success next %v match %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		} else if reply.Conflict {
			DPrintf("[server %v]: Conflict from %v %#v", rf.me, peer, reply)
			if reply.XTerm == -1 {
				rf.nextIndex[peer] = reply.XLen
			} else {
				lastLogInxTerm := rf.findLastLogInTerm(reply.XTerm)
				DPrintf("[server %v]: lastLogInXTerm %v", rf.me, lastLogInxTerm)
				if lastLogInxTerm > 0 {
					rf.nextIndex[peer] = lastLogInxTerm
				} else {
					rf.nextIndex[peer] = reply.XIndex
				}
			}
			DPrintf("[server %v]: leader nextIndex[%v] %v", rf.me, peer, rf.nextIndex[peer])
		} else if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer]--
		}
		rf.leaderCommitRule()

	}

}

func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log.lastLog().Index; i > 0; i-- {
		if rf.log.at(i).Term == x {
			return i
		} else if rf.log.at(i).Term < x {
			break
		}
	}
	return -1
}
func (rf *Raft) leaderCommitRule() {
	if rf.State != Leader {
		return
	}
	for n := rf.commitIndex + 1; n <= rf.log.lastLog().Index; n++ {
		if rf.log.at(n).Term != rf.currentTerm {
			continue
		}
		counter := 1
		for peer := 0; peer < len(rf.peers); peer++ {
			if peer != rf.me && rf.matchIndex[peer] >= n {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n
				DPrintf("[server %v] leader尝试提交 index %v", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}

}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[server %v] term %d follower get %v AppendEntries %v", rf.me, rf.currentTerm, args.LeaderId, args.Entries)

	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()

	if rf.State == Candidate {
		rf.State = Follower
	}

	if rf.log.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XIndex = -1
		reply.XTerm = -1
		reply.XLen = rf.log.len()
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		Xterm := rf.log.at(args.PrevLogIndex).Term

		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log.at(xIndex-1).Term != Xterm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = Xterm
		reply.XLen = rf.log.len()

		DPrintf("[server %v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	for idx, entry := range args.Entries {
		if entry.Index <= rf.log.lastLog().Index && rf.log.at(entry.Index).Term != entry.Term {
			rf.log.truncate(entry.Index)
		}

		if entry.Index > rf.log.lastLog().Index {
			rf.log.append(args.Entries[idx:]...)
			DPrintf("[server %d]: follower append [%v]", rf.me, args.Entries[idx:])
			break

		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.log.lastLog().Index, args.LeaderCommit)
		rf.apply()
	}
	reply.Success = true

}
