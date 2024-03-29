package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"errors"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

const ElectionTimeout time.Duration = time.Duration(1000 * time.Millisecond)
const AppendEntriesInterval time.Duration = time.Duration(100 * time.Millisecond)

func randDuration(min time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % min
	return time.Duration(min + extra)
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	timer.Stop()
	timer.Reset(duration)
}

func afterBetween(min time.Duration, max time.Duration) <-chan time.Time {
	d, delta := min, max-min
	if delta > 0 {
		d = d + time.Duration(rand.Int63())%min
	}
	return time.After(d)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	leaderId    int
	log         []LogEntry
	logIndex    int // next log index

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state         ServerState
	applyCh       chan ApplyMsg
	shutdownCh    chan struct{}
	notifyApplyCh chan struct{}

	c chan *ev

	electionTimer *time.Timer
}

func (rf *Raft) serverState() ServerState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) loop() {
	state := rf.serverState()
	for state != Stopped {
		switch state {
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}
		state = rf.serverState()
	}
}

func (rf *Raft) followerLoop() {
	timeoutChan := afterBetween(ElectionTimeout, 2*ElectionTimeout)
	for rf.serverState() == Follower {
		update := false
		var err error
		select {
		case <-rf.shutdownCh:
			rf.state = Stopped
		case e := <-rf.c:

			switch req := e.target.(type) {
			case *RequestVoteArgs:
				e.returnValue, update = rf.processRequestVote(req)
			case *AppendEntriesArgs:
				e.returnValue, update = rf.processAppendEntries(req)
			default:
				err = errors.New("No Supported Request")
			}
			e.err <- err
		case <-timeoutChan:
			rf.state = Candidate
		}

		if update {
			timeoutChan = afterBetween(ElectionTimeout, 2*ElectionTimeout)
		}
	}
}

func (rf *Raft) candidateLoop() {

	var replyCh chan *RequestVoteReply
	var timeoutCh <-chan time.Time
	var err error
	lastLog := rf.log[rf.logIndex-1]
	doVote := true
	count := 0

	for rf.serverState() == Candidate {
		if doVote {

			rf.currentTerm++
			rf.votedFor = rf.me

			replyCh = make(chan *RequestVoteReply)
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLog.LogIndex,
				LastLogTerm:  lastLog.LogTerm,
			}

			for i := range rf.peers {
				if i != rf.me {
					go func() {
						requestVoteReply := &RequestVoteReply{}
						rf.sendRequestVote(i, args, requestVoteReply)
						replyCh <- requestVoteReply
					}()
				}
			}
			timeoutCh = afterBetween(ElectionTimeout, ElectionTimeout*2)
			doVote = false
			count = 1
		}

		if count >= len(rf.peers)/2+1 {
			rf.state = Leader
		}

		select {
		case <-rf.shutdownCh:
			rf.state = Stopped
		case reply := <-replyCh:
			if reply.VoteGranted {
				count++
			} else {
				if reply.Term > rf.currentTerm {
					if rf.state == Leader {
						// todo stop heartbeat
					}
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.leaderId = -1
				}
			}

		case e := <-rf.c:
			switch req := e.target.(type) {
			case *RequestVoteArgs:
				e.target, _ = rf.processRequestVote(req)
			case *AppendEntriesArgs:
				e.target, _ = rf.processAppendEntries(req)
			default:
				err = errors.New("No Supported Request")
			}

			e.err <- err
		case <-timeoutCh:
			doVote = true
		}
	}

}

func (rf *Raft) leaderLoop() {

}

func (rf *Raft) campaign() {

	// step 1.
	// increment term, transitions to candidate, vote itself, reset timer
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	lastLog := rf.log[len(rf.log)-1]
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLog.LogIndex,
		LastLogTerm:  lastLog.LogTerm,
	}
	electionDuration := randDuration(ElectionTimeout)
	timeout := time.After(electionDuration)
	resetTimer(rf.electionTimer, electionDuration)
	rf.mu.Unlock()

	// step 2.
	// issues RequestVote RPCs in parallel to each of the other servers
	requestReplyCh := make(chan *RequestVoteReply)
	for i := range rf.peers {
		if i != rf.me {
			go func() {
				requestVoteReply := &RequestVoteReply{}
				rf.sendRequestVote(i, requestVoteArgs, requestVoteReply)
				requestReplyCh <- requestVoteReply
			}()
		}
	}

	count, threshold := 1, len(rf.peers)/2
	for count < threshold {
		select {
		case <-rf.shutdownCh:
			return
		case <-timeout:
			return
		case reply := <-requestReplyCh:
			if reply.VoteGranted {
				count++
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					// If RPC request or response contains term T > currentTerm
					// set currentTerm = T, convert to follower
					rf.stepDown(reply.Term)
				}
				rf.mu.Unlock()
			}
		}
	}

	rf.mu.Lock()
	if rf.state == Candidate {
		rf.state = Leader
		rf.initIndex()
		go rf.notifyNewLeader()
		go rf.heartbeat()
	}
	rf.mu.Unlock()
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	resetTimer(rf.electionTimer, randDuration(ElectionTimeout))
}

func (rf *Raft) sendLogEntry(server int) {

}

func (rf *Raft) heartbeat() {
	timer := time.NewTimer(AppendEntriesInterval)
	for {
		select {
		case <-rf.shutdownCh:
			return
		case <-timer.C:
			if _, leader := rf.GetState(); !leader {
				// so it's not leader, stop heartbeat
				return
			}
			go rf.replicate()
			timer.Reset(AppendEntriesInterval)
		}
	}
}

func (rf *Raft) notifyNewLeader() {

}

func (rf *Raft) replicate() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendLogEntry(i)
		}
	}
}

func (rf *Raft) initIndex() {
	l := len(rf.peers)
	rf.nextIndex, rf.matchIndex = make([]int, l), make([]int, l)
	for i := 0; i < l; i++ {
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0
	}

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendRetries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderId = -1

	rf.log = []LogEntry{{0, 0, nil}}
	rf.logIndex = 1
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers)-1)
	rf.matchIndex = make([]int, len(peers)-1)

	rf.state = Follower
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 100)
	rf.shutdownCh = make(chan struct{})
	rf.c = make(chan *ev, 256)
	rf.electionTimer = time.NewTimer(randDuration(ElectionTimeout))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.loop()

	return rf
}
