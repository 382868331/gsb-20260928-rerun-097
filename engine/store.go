package engine

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// Store 是只追加的历史事件存储。运行事实一旦写入就不可修改，
// 执行器的全部进度都必须能通过重放这些事件推导出来。
type Store interface {
	Append(evt Event) error
	Read() ([]Event, error)
}

// FileStore 把历史按 JSON Lines 落盘，每条事件一行并立即刷盘。
// 新建执行器时读取同一文件即可恢复，模拟“进程重新创建”。
type FileStore struct {
	mu sync.Mutex
	f  *os.File
}

func OpenFileStore(path string) (*FileStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileStore{f: f}, nil
}

func (s *FileStore) Append(evt Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := s.f.Write(data); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *FileStore) Read() ([]Event, error) {
	s.mu.Lock()
	f, err := os.Open(s.f.Name())
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var evt Event
		if err := json.Unmarshal(line, &evt); err != nil {
			return nil, err
		}
		events = append(events, evt)
	}
	return events, sc.Err()
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}

// MemoryStore 主要用于测试。
type MemoryStore struct {
	mu     sync.Mutex
	events []Event
}

func (s *MemoryStore) Append(evt Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evt)
	return nil
}

func (s *MemoryStore) Read() ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out, nil
}
