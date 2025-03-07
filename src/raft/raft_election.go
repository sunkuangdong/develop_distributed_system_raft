package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) resetElectionTimeout() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "%d, Can't vote for peer %d, lower term: T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// Check for votedFor
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		LOG(rf.me, rf.currentTerm, DVote, "%d, Can't vote for peer %d, already voted for %d", args.CandidateId, rf.votedFor)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimeout()
	LOG(rf.me, rf.currentTerm, DVote, "%d, Voted for peer %d", args.CandidateId, args.CandidateId)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection(term int) {
	votes := 0

	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Can't send RequestVote to peer %d", peer)
			return
		}

		// Become follower if peer has a higher term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// Context lost if we are candidate and peer has a higher term
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Context lost, can't vote for peer %d", peer)
			return
		}

		// Vote for peer if they have a higher term
		if reply.VoteGranted {
			votes++

			// Become leader if majority of peers voted for us
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Context lost if we are follower and peer has a higher term
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Context lost, can't start election")
		return
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}

		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		go askVoteFromPeer(peer, args)
	}
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
