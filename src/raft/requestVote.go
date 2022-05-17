package raft

import "sync"

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term      int
	VoteGrand bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGrand = false
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term {
		rf.setNewTerm(args.Term)
	}
	lastLog := rf.log.lastLog()
	judge := CheckOutLogUptoDate(lastLog, args)
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && judge {
		rf.voteFor = args.CandidateId
		reply.VoteGrand = true
		rf.resetElectionTimer()
		DPrintf("[server %v] term %v vote %v", rf.me, rf.currentTerm, args.CandidateId)
	}

}

func CheckOutLogUptoDate(lastLog *Entry, args *RequestVoteArgs) bool {
	if args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index) {
		return true
	} else {
		return false
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) candidateRequestVote(peer int, args *RequestVoteArgs, counter *int, becomeLeader *sync.Once) {
	DPrintf("[server %v] send request vote to %d term %v", rf.me, peer, args.Term)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peer, args, &reply)
	if !ok {
		DPrintf("bad return send requestvote")
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < reply.Term {
		DPrintf("[server %v] term %d :too small ", rf.me, rf.currentTerm)
		rf.setNewTerm(reply.Term)
		return
	}

	if !reply.VoteGrand {
		DPrintf("[server %v] %d did not vote to ", rf.me, peer)
		return
	}
	*counter++

	if *counter > len(rf.peers)/2 && rf.State == Candidate && rf.currentTerm == args.Term {
		DPrintf("[server %v] get most vote and return", rf.me)
		becomeLeader.Do(func() {
			rf.State = Leader
			lastLogIndex := rf.log.lastLog().Index
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.nextIndex[i] = 0
			}
			DPrintf("[server %v] become leader and try to send empty entries", rf.me)
			rf.appendEntries(true)
		})
	}

}
