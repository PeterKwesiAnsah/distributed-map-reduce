package mr

import (
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestMain builds the word-count plugin and the coordinator/worker/sequential
// binaries so that `go test ./mr/` works from a clean checkout. These are
// gitignored build artifacts (mr-* and *.so), not source files.
func TestMain(m *testing.M) {
	repoRoot, err := filepath.Abs(filepath.Join(".."))
	if err != nil {
		log.Fatalf("resolve repo root: %v", err)
	}
	builds := []struct {
		output string
		args   []string
	}{
		{"wc.so", []string{"-buildmode=plugin", "-o", "wc.so", "mrapps/wc.go"}},
		{"mrsequential", []string{"-o", "mrsequential", "mrsequential.go", "plugin_load.go"}},
		{"mrcoordinator", []string{"-o", "mrcoordinator", "mrcoordinator.go"}},
		{"mrworker", []string{"-o", "mrworker", "mrworker.go", "plugin_load.go"}},
	}
	for _, b := range builds {
		cmd := exec.Command("go", append([]string{"build"}, b.args...)...)
		cmd.Dir = repoRoot
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatalf("build %s: %v", b.output, err)
		}
	}
	os.Exit(m.Run())
}

func TestWc(t *testing.T) {
	mkOut()
	t.Cleanup(func() {
		if !t.Failed() {
			cleanup()
		}
	})

	app := "../../wc.so"
	files := findFilesPre("../", "pg*txt", "..")

	// 1. Produce the correct output with the sequential implementation.
	mkCorrectOutput(files, app, "mr-wc-correct.txt")

	// 2. Run the distributed MapReduce (one coordinator + workers in parallel).
	runMR(files, app, 3)

	// 3. Merge the per-reduce outputs and compare against the sequential baseline.
	mergeOutput("mr-wc-all.txt")
	runCmp(t, "mr-wc-all.txt", "mr-wc-correct.txt",
		"incorrect combined reduce output: mr-wc-all.txt vs mr-wc-correct.txt")
}

