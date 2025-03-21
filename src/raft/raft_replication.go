package raft

import (
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	CommandValid bool        // if it should be applied to the state machine
	Command      interface{} // the command to be applied to the state machine
	Term         int         // the term of the log entry
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// used to probe the matching entries
	PrevLogIndex int
	PrevLogTerm  int

	// log entries to append
	Entries []LogEntry

	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev=[%d]T%d, (%d, %d], CommitIdx=[%d]",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConfilictIndex int
	ConfilictTerm  int
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Success=%v, ConfilictTerm=[%d]T%d",
		reply.Term, reply.Success, reply.ConfilictTerm, reply.ConfilictIndex)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<-S%d, Append, Args: %v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "AppendEntries from %d, lower term: T%d", args.LeaderId, args.Term)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	defer func() {
		rf.resetElectionTimeout()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<-S%d, Follwer Conflict: [%d]T%d", args.LeaderId, reply.ConfilictIndex, reply.ConfilictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<-S%d, Follower log: %v", args.LeaderId, rf.logString())
		}
	}()

	// return failure if prevlog not match
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConfilictIndex = len(rf.log)
		reply.ConfilictTerm = InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog2, "AppendEntries from %d, prevlog not match: %d", args.LeaderId, args.PrevLogIndex)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConfilictIndex = args.PrevLogIndex
		reply.ConfilictTerm = rf.firstLogFor(reply.ConfilictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "AppendEntries from %d, prevlog not match: %d", args.LeaderId, args.PrevLogIndex)
		return
	}

	// append the leader's log entries
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "AppendEntries from %d, success: %d", args.LeaderId, args.PrevLogIndex)

	// TODO: update nextIndex and matchIndex
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DLog2, "Update commitIndex to %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}

}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexes))
	majorityIdx := (len(rf.peers) - 1) / 2

	LOG(rf.me, rf.currentTerm, DDebug, "Majority index: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
}

func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "Can't send AppendEntries to peer %d", peer)
			return
		}

		LOG(rf.me, rf.currentTerm, DDebug, "<-S%d, Append, Reply: %v", peer, reply.String())

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "->%d: Lost leader to T%d: leader->T%d:%d", peer, term, rf.currentTerm, rf.role)
			return
		}

		if !reply.Success {
			prevIndex := rf.nextIndex[peer]
			if reply.ConfilictTerm == InvalidTerm {
				prevIndex = reply.ConfilictIndex
			} else {
				firstIndex := rf.firstLogFor(reply.ConfilictTerm)
				if firstIndex != InvalidIndex {
					rf.nextIndex[peer] = firstIndex
				} else {
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}

			if prevIndex < rf.nextIndex[peer] {
				rf.nextIndex[peer] = prevIndex
			}

			LOG(rf.me, rf.currentTerm, DLog, "->%d: Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[peer]-1, rf.log[rf.nextIndex[peer]-1])
			LOG(rf.me, rf.currentTerm, DDebug, "->%d, Leader log: %v", peer, rf.logString())
			return
		}

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// TODO: update commitIndex
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log[majorityMatched].Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DLog2, "Leader commitIndex to %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost leader to %s[T%d]", rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:],
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "->S%d, Append, Args: %v", peer, args.String())
		go replicateToPeer(peer, args)
	}

	return true
}

func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}

		time.Sleep(replicateInterval)
	}
}
