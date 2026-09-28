package flow

import (
	"encoding/json"
	"os"
)

type Definition struct {
	ID    string              `json:"id"`
	Start string              `json:"start"`
	Nodes map[string]NodeSpec `json:"nodes"`
}

type NodeSpec struct {
	Kind      string `json:"kind"`
	Action    string `json:"action,omitempty"`
	Next      string `json:"next,omitempty"`
	Result    string `json:"result,omitempty"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
	OnResult  string `json:"on_result,omitempty"`
	OnTimeout string `json:"on_timeout,omitempty"`
}

func LoadDefinition(path string) (*Definition, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var definition Definition
	if err := json.Unmarshal(data, &definition); err != nil {
		return nil, err
	}
	return &definition, nil
}
