package raft

import "fmt"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, RecvSnapshot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog, "<- S%d, Reject Snap, Higher Term: T%d>T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	if rf.log.sanpLastIdx >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DLog, "<- S%d, Reject Snap, Already Installed: %d >= %d", args.LeaderId, rf.log.sanpLastIdx, args.LastIncludedIndex)
		return
	}

	rf.log.InstallSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DLog, "<- S%d, InstallSnapshot, Args=%v, Accept, T%d", args.LeaderId, args.String(), rf.currentTerm)

	rf.snapPending = true
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallToPeer(peer int, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		LOG(rf.me, rf.currentTerm, DLog, "Can't send InstallSnapshot to peer %d", peer)
	}
	LOG(rf.me, rf.currentTerm, DLog, "<- S%d, RecvSnapshot, Args=%v", peer, args.String())

	if reply.Term < rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost leader, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
		return
	}

	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}

}
