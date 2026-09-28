package flow

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type Event struct {
	Seq        int64                  `json:"seq"`
	TimeNS     int64                  `json:"time_ns"`
	RunID      string                 `json:"run_id"`
	Type       string                 `json:"type"`
	Node       string                 `json:"node,omitempty"`
	Next       string                 `json:"next,omitempty"`
	Action     string                 `json:"action,omitempty"`
	Result     string                 `json:"result,omitempty"`
	Attempt    int                    `json:"attempt,omitempty"`
	DeadlineNS int64                  `json:"deadline_ns,omitempty"`
	Detail     string                 `json:"detail,omitempty"`
	Data       map[string]interface{} `json:"data,omitempty"`
}

type History struct {
	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
	seq     int64
	events  []Event
}

func OpenHistory(path string) (*History, error) {
	if err := os.MkdirAll(pathDir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	h := &History{file: f}
	if err := h.load(); err != nil {
		_ = f.Close()
		return nil, err
	}
	h.encoder = json.NewEncoder(f)
	return h, nil
}

func (h *History) load() error {
	if _, err := h.file.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(h.file)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return err
		}
		h.events = append(h.events, event)
		h.seq = event.Seq
	}
	return scanner.Err()
}

func (h *History) Append(event Event) (Event, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	event.Seq = h.seq
	if event.TimeNS == 0 {
		event.TimeNS = nowUnixNano()
	}
	if err := h.encoder.Encode(event); err != nil {
		h.seq--
		return Event{}, err
	}
	if err := h.file.Sync(); err != nil {
		h.seq--
		return Event{}, err
	}
	h.events = append(h.events, event)
	return event, nil
}

func (h *History) Events() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]Event, len(h.events))
	copy(out, h.events)
	return out
}

func (h *History) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.file.Close()
}
