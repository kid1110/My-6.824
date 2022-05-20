package raft

type InstallSnapShotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Offset           int
	data             []byte
	Done             bool
}

type InstallSnapShotReply struct {
	Term int
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("snapshot until %d", index)
	for _, entry := range rf.log.Entries {
		if entry.Index == index {
			DPrintf("before snapshot: full log:%v", rf.log.Entries)
			rf.log.Entries = rf.log.slice(index)
			rf.log.trim(index)
			rf.persister.SaveStateAndSnapshot(rf.savePersist(), snapshot)
			return
		}

	}

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}
func (rf *Raft) leaderSendInstallSnapShot() {

}
func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[server %v] receive term %d small than currentTerm %d reject InstallSnapshot", rf.me, args.Term, rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	if args.Offset == 0 {

	}

}
