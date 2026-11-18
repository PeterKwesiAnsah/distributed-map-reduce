# distributed-map-reduce

A distributed MapReduce framework in Go, inspired by the MIT 6.5840 / 6.824 MapReduce lab. A single coordinator assigns map and reduce tasks to multiple worker processes over HTTP-over-Unix-socket RPC, handles worker failures via heartbeats, and runs user-supplied Map/Reduce logic loaded from Go plugins.

**Module:** `distributed-map-reduce` — Go 1.22

---

## Table of contents

1. [Overview](#overview)
2. [Features](#features)
3. [Project structure](#project-structure)
4. [Prerequisites](#prerequisites)
5. [Quick start](#quick-start)
6. [Running the sequential version](#running-the-sequential-version)
7. [Running the tests](#running-the-tests)
8. [Writing your own MapReduce plugin](#writing-your-own-mapreduce-plugin)
9. [Included applications](#included-applications)
10. [Output file conventions](#output-file-conventions)
11. [Architecture notes](#architecture-notes)
12. [Roadmap / open TODOs](#roadmap--open-todos)

---

## Overview

This project implements the classic MapReduce programming model:

* **Map** — each input file is split into chunks; a map task reads one chunk and calls the user's `Map(filename, contents)` function, which emits `[]mr.KeyValue` pairs.
* **Partition** — intermediate key/value pairs are partitioned by `ihash(key) % nReduce` into per-reduce intermediate files named `mr-X-Y` (map task `X`, reduce partition `Y`).
* **Reduce** — each reduce task gathers all intermediate files for its partition, sorts by key, and calls the user's `Reduce(key, values)` once per distinct key, writing `mr-out-Y`.

The coordinator and workers are separate OS processes. The coordinator exposes an RPC service over a UNIX domain socket (via `net/rpc` + HTTP), and workers poll it for tasks. When all reduce tasks finish, the coordinator marks the job done.

---

## Features

* **Distributed execution** — one coordinator plus any number of worker processes.
* **UNIX-socket RPC** — coordinator and workers communicate via `net/rpc` over HTTP on a Unix domain socket.
* **Fault tolerance** — the coordinator heartbeats workers every second and reassigns map/reduce tasks from workers that fail to respond within 10 seconds.
* **Plugin architecture** — user Map/Reduce functions are compiled as Go plugins (`.so`) and loaded at runtime by both workers and the sequential runner.
* **Sequential reference** — `mrsequential.go` runs the whole job in a single process, useful for producing correct output to compare against.
* **Test harness** — `mr/mr_test.go` + `mr/util.go` automate coordinator/worker startup, output merging, and comparison against the sequential baseline.

---

## Project structure

```
distributed-map-reduce/
├── go.mod                 # module distributed-map-reduce, go 1.22
├── README.md
├── .gitignore             # ignores mr-* and *.so artifacts
├── mrcoordinator.go       # coordinator entry point: MakeCoordinator(sock, files, nReduce)
├── mrworker.go            # worker entry point: Worker(sock, mapf, reducef)
├── mrsequential.go        # single-process MapReduce reference
├── plugin_load.go         # loads Map/Reduce from a .so plugin
├── pg-*.txt               # Project Gutenberg input texts (8 files)
├── wc.so                  # pre-built wc plugin
├── sock123                # leftover unix socket
├── mr/                    # shared framework package
│   ├── coordinator.go     # Coordinator: task scheduling, heartbeats, failure recovery, Done()
│   ├── worker.go          # Worker(): RPC task loop, map/reduce execution, output writing
│   ├── rpc.go             # RPC types: Task, TaskType, KeyValue, Chunk, TaskReport, …
│   ├── bitmap.go          # Bitmap bitset used for task tracking
│   ├── util.go            # test helpers (runMR, coordinatorSock, mergeOutput, …)
│   ├── mr_test.go         # integration tests (TestWc enabled, others commented out)
│   └── mr-tmp-*           # temporary test output directories
└── mrapps/                # example MapReduce plugins
    ├── wc.go              # word count
    ├── indexer.go         # inverted index (word -> docs)
    ├── crash.go           # randomly crashes / delays (fault injection)
    ├── early_exit.go      # slow reduce on some keys (early-exit test)
    ├── jobcount.go        # counts map task invocations
    ├── mtiming.go         # map timing (parallelism test)
    ├── nocrash.go         # non-crashing variant
    └── rtiming.go         # reduce timing (parallelism test)
```

---

## Prerequisites

* **Go 1.22.0** or newer (`go version`)

Plugins are built with `go build -buildmode=plugin`, which on some systems requires CGO to be enabled.

---

## Quick start

This example runs the included word-count application (`wc`) across all eight Gutenberg text files with 3 reduce tasks.

### 1. Build the plugin

```bash
cd mrapps
go build -buildmode=plugin -o ../wc.so wc.go
cd ..
```

(`wc.so` is already included in the repo, so this step is optional unless you modify `wc.go`.)

### 2. Start the coordinator

The coordinator takes a socket name followed by the input files:

```bash
go run mrcoordinator.go sock123 pg-*.txt &
```

It prints `Coordinator: Listening on sock123` and runs until `Done()` returns true.

### 3. Start workers

In separate terminals, start one or more workers. Each worker takes the plugin path and the socket name:

```bash
go run mrworker.go wc.so sock123
```

Run this 2–4 times in parallel to see map/reduce tasks distributed across workers.

### 4. Inspect the output

When the coordinator exits, you'll find one output file per reduce task:

```
mr-out-0  mr-out-1  mr-out-2
```

Each line is `<key> <value>` (for `wc`, `<word> <count>`).

---

## Running the sequential version

The sequential implementation produces a single `mr-out-0` file and is useful for verifying correctness:

```bash
go run mrsequential.go wc.so pg-*.txt
```

Compare its output against the distributed run by sorting and merging:

```bash
sort mr-out-* > mr-wc-all.txt
sort mr-out-0 > mr-wc-seq.txt   # from mrsequential.go output
cmp mr-wc-all.txt mr-wc-seq.txt
```

---

## Running the tests

The framework package includes an integration test (`TestWc`) that starts a coordinator and workers, merges the reduce output, and compares it against the sequential baseline.

```bash
go test ./mr/ -run TestWc -v
```

Other tests (`TestIndexer`, `TestCrashWorker`, `TestEarlyExit`, parallelism tests, etc.) are present in `mr/mr_test.go` but commented out.

---

## Writing your own MapReduce plugin

Create a Go file in `package main` with the two required functions:

```go
package main

import "distributed-map-reduce/mr"

// Map is called once per input chunk.
func Map(filename string, contents string) []mr.KeyValue {
    // return []mr.KeyValue{{key, value}, ...}
}

// Reduce is called once per distinct key with all its values.
func Reduce(key string, values []string) string {
    // return a single string result
}
```

Build it as a plugin:

```bash
go build -buildmode=plugin -o myapp.so myapp.go
```

Then run:

```bash
go run mrcoordinator.go sock123 pg-*.txt &
go run mrworker.go myapp.so sock123   # repeat for multiple workers
```

---

## Included applications

| Plugin | Description |
|--------|-------------|
| `wc.go` | Word count — counts occurrences of each word. |
| `indexer.go` | Inverted index — each word maps to the documents it appears in. |
| `crash.go` | Fault injection — randomly crashes or delays (tests recovery). |
| `early_exit.go` | Slow reduce on some keys — tests that workers/coordinator don't exit early. |
| `jobcount.go` | Counts map task invocations — detects duplicate task execution. |
| `mtiming.go` | Verifies map tasks run in parallel. |
| `nocrash.go` | Non-crashing variant used by crash tests. |
| `rtiming.go` | Verifies reduce tasks run in parallel. |

---

## Output file conventions

* **Intermediate files:** `mr-X-Y` — produced by map task `X` for reduce partition `Y`, stored as JSON-encoded `mr.KeyValue` records.
* **Final output:** `mr-out-Y` — one per reduce task `Y`, each line `<key> <value>`.
* The sequential runner writes only `mr-out-0`.
* `.gitignore` ignores `mr-*` and `*.so` to keep build/test artifacts out of version control.

---

## Architecture notes

* **Coordinator** (`mr/coordinator.go`)
  * Serves RPC methods `GetTask`, `GetWorkerConfig`, `ReportTaskFailed`.
  * Assigns pending map tasks first; reduce tasks start only after all map outputs for their partition are available.
  * Tracks task state with a `Bitmap` (scheduled / received / completed).
  * Heartbeats every 1s; workers silent for more than 10s are treated as dead and their in-flight tasks are rescheduled.
  * `Done()` returns true once every reduce task is complete.
* **Worker** (`mr/worker.go`)
  * Exposes its own RPC endpoint (`WorkerTaskState.HeartbeatPing`) so the coordinator can poll it.
  * Loops: request task → execute (map or reduce) → report completion → repeat until `ExitTask`.
  * Map writes partitioned intermediate files `mr-X-Y`; reduce reads them, sorts by key, and writes `mr-out-Y`.
  * Map task assignment uses `ihash(key) % nReduce` for partitioning.
* **Task types:** `MapTask`, `ReduceTask`, `WaitTask` (no work available yet), `ExitTask` (job finished).

---

## Roadmap / open TODOs

The source tree contains the following outstanding items (from `mr/coordinator.go`):

* Backup tasks (speculative execution for stragglers)
* Ordering guarantees
* Combiner function (local pre-reduction to reduce network/intermediate data)
* Status information / counters
* Per-task locking to replace the global coordinator lock

---

## License

This project is a personal/educational implementation of the MapReduce pattern. See the repository for any applicable license.