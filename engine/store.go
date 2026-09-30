package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store 是只追加的历史落盘。每个 run 一个 JSONL 文件，
// 事件按实际发生顺序写入，恢复时按序重放。
type Store struct {
	dir string
	mu  sync.Mutex
}

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func (s *Store) path(runID string) string {
	return filepath.Join(s.dir, "run-"+runID+".jsonl")
}

// Append 追加一条事件并落盘（fsync），保证崩溃后不丢已确认的历史。
func (s *Store) Append(e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.path(e.RunID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// Load 按落盘顺序读出某个 run 的全部历史。
func (s *Store) Load(runID string) ([]Event, error) {
	f, err := os.Open(s.path(runID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var events []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("corrupt history line: %w", err)
		}
		events = append(events, e)
	}
	return events, sc.Err()
}

// Runs 列出落盘过的所有 runID。
func (s *Store) Runs() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		name := e.Name()
		if len(name) > 9 && name[:4] == "run-" && name[len(name)-6:] == ".jsonl" {
			ids = append(ids, name[4:len(name)-6])
		}
	}
	return ids, nil
}
