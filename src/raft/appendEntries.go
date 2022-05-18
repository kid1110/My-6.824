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
	Term    int
	Success bool
}

func (rf *Raft) appendEntries(heartbeat bool) {

	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}

		if heartbeat {
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				Entries:  make([]Entry, 0),
			}
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

}
