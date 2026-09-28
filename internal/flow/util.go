package flow

import "path/filepath"

func pathDir(path string) string {
	dir := filepath.Dir(path)
	if dir == "." {
		return "."
	}
	return dir
}
