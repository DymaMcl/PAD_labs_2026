package broker

import "time"

// Event este o intrare din jurnalul de observabilitate al brokerului
// (afisat in dashboard). Nu este folosit pentru recuperare.
type Event struct {
	N      uint64    `json:"n"`
	At     time.Time `json:"at"`
	Queue  string    `json:"queue"`
	Kind   string    `json:"kind"`
	MsgID  string    `json:"msg_id,omitempty"`
	Key    string    `json:"key,omitempty"`
	Detail string    `json:"detail,omitempty"`
}

// eventLog este un buffer circular de evenimente.
type eventLog struct {
	buf  []Event
	cap  int
	next uint64
}

func newEventLog(capacity int) *eventLog { return &eventLog{cap: capacity} }

func (l *eventLog) add(at time.Time, queue, kind, msgID, key, detail string) {
	l.next++
	l.buf = append(l.buf, Event{N: l.next, At: at, Queue: queue, Kind: kind, MsgID: msgID, Key: key, Detail: detail})
	if len(l.buf) > l.cap {
		l.buf = l.buf[len(l.buf)-l.cap:]
	}
}

func (l *eventLog) since(n uint64) []Event {
	out := []Event{}
	for _, e := range l.buf {
		if e.N > n {
			out = append(out, e)
		}
	}
	return out
}
