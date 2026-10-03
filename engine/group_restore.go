package engine

import (
	"strconv"
	"strings"
)

func findGroupByToken(root *groupState, token string) *groupState {
	if root == nil {
		return nil
	}
	if root.token == token {
		return root
	}
	for _, c := range root.children {
		if c != nil {
			if g := findGroupByToken(c, token); g != nil {
				return g
			}
		}
	}
	return nil
}

// parseAttempt 解析 "attempt=N" 形式的 Detail/Result。
func parseAttempt(s string) int {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "attempt=") {
		return 0
	}
	n, err := strconv.Atoi(s[len("attempt="):])
	if err != nil {
		return 0
	}
	return n
}

func parseSelected(extra map[string]interface{}) []string {
	if extra == nil {
		return nil
	}
	v, ok := extra["selected"]
	if !ok {
		return nil
	}
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	var out []string
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// restoreBranchTerminal 按历史恢复分支终态；迟到结果只进 lates。
func restoreBranchTerminal(b *branchState, kind, result string, late bool) {
	if late || b.terminal {
		b.lates = append(b.lates, branchOutcome{kind: kind, result: result, attempt: b.attempt})
		return
	}
	b.terminal = true
	b.outcome = &branchOutcome{kind: kind, result: result, attempt: b.attempt}
}
