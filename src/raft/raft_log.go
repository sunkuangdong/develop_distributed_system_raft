package raft

import (
	"course/labgob"
	"fmt"
)

type RaftLog struct {
	sanpLastIdx  int
	sanpLastTerm int

	// contains the snapshot
	snapshot []byte
	// contains index (snapLastIdx, snapLastIdx + len(tailLog)-1) for real data
	tailLog []LogEntry
}

func NewLog(snapLastIdx int, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		sanpLastIdx:  snapLastIdx,
		sanpLastTerm: snapLastTerm,
		snapshot:     snapshot,
	}

	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})

	rl.tailLog = append(rl.tailLog, entries...)
	return rl
}

func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lasIdx int
	if err := d.Decode(&lasIdx); err != nil {
		return fmt.Errorf("Decode last include index failed")
	}
	rl.sanpLastIdx = lasIdx

	var lasTerm int
	if err := d.Decode(&lasTerm); err != nil {
		return fmt.Errorf("Decode last include term failed")
	}
	rl.sanpLastTerm = lasTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("Decode log entries failed")
	}
	rl.tailLog = log

	return nil
}

func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.sanpLastIdx)
	e.Encode(rl.sanpLastTerm)
	e.Encode(rl.tailLog)
}

func (rl *RaftLog) size() int {
	return rl.sanpLastIdx + len(rl.tailLog)
}

func (rl *RaftLog) idx(logicIdx int) int {
	if logicIdx < rl.sanpLastIdx || logicIdx > rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIdx, rl.sanpLastIdx, rl.size()-1))
	}
	return logicIdx - rl.sanpLastIdx
}

func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

func (rl *RaftLog) last() (index int, term int) {
	i := len(rl.tailLog) - 1
	return rl.sanpLastIdx + i, rl.tailLog[i].Term
}

func (rl *RaftLog) lastIdx() int {
	return rl.sanpLastIdx + len(rl.tailLog)
}

func (rl *RaftLog) firstFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return rl.sanpLastIdx + idx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog) tail(startIdx int) []LogEntry {
	if startIdx >= rl.size() {
		return nil
	}

	return rl.tailLog[rl.idx(startIdx):]
}

func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

func (rl *RaftLog) appendFrom(logicPrevIndex int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicPrevIndex)+1], entries...)
}

func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.sanpLastTerm
	prevStart := rl.sanpLastIdx
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, rl.sanpLastIdx+i-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, rl.sanpLastIdx+len(rl.tailLog)-1, prevTerm)
	return terms
}

func (rl *RaftLog) InstallSnapshot(lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) {
	rl.sanpLastIdx = lastIncludedIndex
	rl.sanpLastTerm = lastIncludedTerm
	rl.snapshot = snapshot

	newLog := make([]LogEntry, 0, rl.size()-rl.sanpLastIdx)
	newLog = append(newLog, LogEntry{
		Term: rl.sanpLastTerm,
	})
	rl.tailLog = newLog
}
