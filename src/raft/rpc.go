package raft

// RPC for RequestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term == rf.currentTerm && args.CandidateID == rf.votedFor {
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		return
	}

	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && args.CandidateID != rf.votedFor) {
		reply.VoteGranted, reply.Term = false, rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		if rf.state != Follower {
			resetTimer(rf.electionTimer, randDuration(ElectionTimeout))
			rf.state = Follower
		}
	}

	reply.Term = args.Term
	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.log[lastLogIndex].LogTerm

	if lastLogTerm > args.Term ||
		(lastLogTerm == args.Term && lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateID
	reply.VoteGranted = true
	resetTimer(rf.electionTimer, randDuration(ElectionTimeout))

}

// RPC for AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	reply.Term = args.Term
	resetTimer(rf.electionTimer, randDuration(ElectionTimeout))

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}


}