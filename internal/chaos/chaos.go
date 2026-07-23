package chaos

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	points   = map[string]bool{}
	once     = true
	stateDir = ".chaos"
)

// The scenario owns the crash-point vocabulary so adding a boundary cannot be
// accidentally blocked by a second list here.
func Init() {
	spec := os.Getenv("CHAOS_CRASH_AT")
	for _, p := range strings.Split(spec, ",") {
		if p = strings.TrimSpace(p); p != "" {
			points[p] = true
		}
	}
	if os.Getenv("CHAOS_ONCE") == "0" {
		once = false
	}
	if d := os.Getenv("CHAOS_STATE_DIR"); d != "" {
		stateDir = d
	}
	if len(points) > 0 {
		fmt.Fprintf(os.Stderr, "[chaos] armed crash points: %v (once=%v)\n", spec, once)
	}
}

func Crash(point string) {
	if !points[point] {
		return
	}
	marker := filepath.Join(stateDir, point)
	if once {
		if _, err := os.Stat(marker); err == nil {
			return
		}
		_ = os.MkdirAll(stateDir, 0o755)
		_ = os.WriteFile(marker, []byte("fired\n"), 0o644)
	}
	fmt.Fprintf(os.Stderr, "[chaos] CRASH at %q — exiting 137\n", point)
	os.Exit(137)
}
