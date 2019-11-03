package raft

// RPC for RequestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.send(args, reply)
}

// RPC for AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.send(args, reply)
}

func (rf *Raft) send(args interface{}, reply interface{}) {
	e := &ev{
		target:      args,
		returnValue: reply,
		err:          make(chan error),
	}

	rf.c <- e

	select {
	case <-e.ok:
		DPrintf("RPC finish")
	}
}

func (rf *Raft) processRequestVote(args *RequestVoteArgs) (*RequestVoteReply, bool) {

	if args.Term < rf.currentTerm {
		return &RequestVoteReply{
			Term:        rf.currentTerm,
			VoteGranted: false,
		}, false
	}

	if args.Term > rf.currentTerm {
		if rf.state == Leader {
			// todo stop heartbeat
		}
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return &RequestVoteReply{
			Term:        rf.currentTerm,
			VoteGranted: false,
		}, false
	}

	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.log[lastLogIndex].LogTerm

	if lastLogTerm > args.Term ||
		(lastLogTerm == args.Term && lastLogIndex > args.LastLogIndex) {
		return &RequestVoteReply{
			Term:        rf.currentTerm,
			VoteGranted: false,
		}, false
	}

	rf.votedFor = args.CandidateID

	return &RequestVoteReply{
		Term:        rf.currentTerm,
		VoteGranted: true,
	}, true
}

func (rf *Raft) processAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, bool) {

	if args.Term < rf.currentTerm {
		return &AppendEntriesReply{
			Term:          rf.currentTerm,
			Success:       false,
		}, false
	}

	if args.Term == rf.currentTerm {
		if rf.state == Candidate {
			rf.state = Follower
		}
		rf.leaderId = args.LeaderID
	} else {
		if rf.state == Leader {
			// todo stop heartbeat
		}
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = args.LeaderID
	}

	logIndex := rf.logIndex
	prevLogIndex, prevLogTerm := args.PrevLogIndex, args.PrevLogTerm
	if logIndex <= prevLogIndex || rf.log[prevLogIndex].LogTerm != prevLogTerm {
		return &AppendEntriesReply{
			Term:          rf.currentTerm,
			Success:       false,
		}, false
	}

	i := 0
	for ; i < len(args.Entries); i++ {

		if prevLogIndex+1+i >= logIndex {
			break
		}
		if rf.log[prevLogIndex+1+i].LogTerm != args.Entries[i].LogTerm {
			rf.logIndex = prevLogIndex + 1 + i
			rf.log = append(rf.log[:rf.logIndex])
		}
	}

	for ; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex++
	}

	oldCommitIndex := rf.commitIndex
	rf.commitIndex = Max(rf.commitIndex, Min(args.LeaderCommit, prevLogIndex+len(args.Entries)))

	if rf.commitIndex > oldCommitIndex {
		rf.notifyApplyCh <- struct{}{}
	}

	return &AppendEntriesReply{
		Term:          rf.currentTerm,
		Success:       true,
	}, true

}
