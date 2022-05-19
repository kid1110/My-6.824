package raft

import (
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) resetElectionTimer() {
	now := time.Now()
	electionTimeOut := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = now.Add(electionTimeOut)
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.currentTerm
	var isleader bool = rf.State == Leader
	return term, isleader
}

func (rf *Raft) setNewTerm(term int) {
	if rf.currentTerm < term || rf.currentTerm == 0 {
		rf.State = Follower
		rf.currentTerm = term
		rf.voteFor = -1
		rf.persist()
		DPrintf("[server %v] set new Term %v", rf.me, rf.currentTerm)
	}
}
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.State = Candidate
	rf.voteFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
	DPrintf("[server %v] start leader election term is %d", rf.me, rf.currentTerm)
	lastLog := rf.log.lastLog()

	request := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	voteCounter := 1

	var becomeLeader sync.Once

	for peer, _ := range rf.peers {
		if peer != rf.me {
			go rf.candidateRequestVote(peer, &request, &voteCounter, &becomeLeader)
		}
	}

}
