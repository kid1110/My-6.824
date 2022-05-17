package raft

type Entry struct {
	Command interface{}
	Index   int
	Term    int
}
type Log struct {
	Entries  []Entry
	LogIndex int
}

func MakeNewLog() Log {
	data := Log{
		Entries:  make([]Entry, 0),
		LogIndex: 0,
	}
	return data
}

func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
}
func (l *Log) at(idx int) *Entry {
	return &l.Entries[idx]
}

func (l *Log) lastLog() *Entry {
	return l.at(len(l.Entries) - 1)

}
