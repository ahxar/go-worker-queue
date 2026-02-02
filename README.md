# Go Worker Queue

A production-ready bounded worker queue implementation in Go with fixed workers, backpressure, context cancellation, and graceful shutdown.

## Features

- **Fixed Worker Pool**: Configurable number of concurrent worker goroutines
- **Bounded Queue**: Buffered channel provides natural backpressure when capacity is reached
- **Context Support**: Full context cancellation support at all levels
- **Graceful Shutdown**: Choose between graceful (process remaining tasks) or immediate shutdown
- **No Goroutine Leaks**: All workers are tracked and properly cleaned up
- **Zero Allocations**: Hot path has zero allocations per operation
- **Thread-Safe**: Safe for concurrent use

## Installation

```bash
go get github.com/ahxar/go-worker-queue
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    workerqueue "github.com/ahxar/go-worker-queue"
)

func main() {
    // Create queue with 3 workers and capacity of 10
    wq := workerqueue.New(3, 10)

    // Start the workers
    if err := wq.Start(); err != nil {
        panic(err)
    }

    // Submit a task
    task := func(ctx context.Context) error {
        fmt.Println("Processing task...")
        return nil
    }

    if err := wq.Submit(context.Background(), task); err != nil {
        fmt.Printf("Failed to submit: %v\n", err)
    }

    // Graceful shutdown
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := wq.Shutdown(ctx); err != nil {
        fmt.Printf("Shutdown error: %v\n", err)
    }
}
```

## Usage

### Creating a Worker Queue

```go
// Create queue with 5 workers and capacity of 100 tasks
wq := workerqueue.New(5, 100)

// Start the worker goroutines
if err := wq.Start(); err != nil {
    log.Fatal(err)
}
```

### Submitting Tasks

Tasks are simple functions that accept a context and return an error:

```go
task := func(ctx context.Context) error {
    // Your work here
    return nil
}

// Submit with context for timeout/cancellation
ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
defer cancel()

if err := wq.Submit(ctx, task); err != nil {
    log.Printf("Submit failed: %v", err)
}
```

### Backpressure

When the queue is full, `Submit()` blocks until space becomes available or the context is cancelled:

```go
// This will block if queue is full
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

err := wq.Submit(ctx, task)
if err == context.DeadlineExceeded {
    log.Println("Queue is full and submission timed out")
}
```

### Graceful Shutdown

Process all remaining tasks before shutting down:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := wq.Shutdown(ctx); err != nil {
    log.Printf("Shutdown timed out: %v", err)
}
```

### Immediate Stop

Stop workers immediately without processing remaining tasks:

```go
wq.Stop() // Blocks until all workers exit
```

## Architecture

### Components

- **WorkerQueue**: Main struct managing workers and task queue
- **Task**: Function type `func(ctx context.Context) error`
- **Buffered Channel**: Provides bounded capacity and backpressure
- **Context**: Enables cancellation and timeout control
- **WaitGroup**: Tracks worker goroutines for clean shutdown

### Flow

1. **Initialization**: Create queue with worker count and capacity
2. **Start**: Launch worker goroutines that poll the task channel
3. **Submit**: Send tasks to channel (blocks when full)
4. **Execution**: Workers receive and execute tasks with context
5. **Shutdown**: Close channel, wait for workers to complete tasks

## Performance

Benchmarks on Apple M1 Pro:

```
BenchmarkSubmit-8             	13539379	       165.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkWorkerThroughput-8   	12173988	       195.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkConcurrentSubmit-8   	19201401	       129.0 ns/op	       0 B/op	       0 allocs/op
```

Key metrics:

- ~130-200ns per operation
- Zero allocations in hot path
- Scales well with concurrent submissions

## Testing

Run tests with race detector:

```bash
go test -v -race ./...
```

Run benchmarks:

```bash
go test -bench=. -benchmem
```

## Examples

See [example/main.go](example/main.go) for a complete runnable example demonstrating:

- Basic task execution
- Backpressure behavior
- Context cancellation
- Graceful shutdown

Run the example:

```bash
go run example/main.go
```

## Design Decisions

1. **Buffered Channel**: Uses Go's native channel backpressure mechanism
2. **Context Throughout**: Enables fine-grained cancellation control
3. **Separate Shutdown vs Stop**: Allows caller to choose graceful or immediate
4. **Simple Task Type**: Just `func(ctx context.Context) error` - no over-engineering
5. **No Task Return Handling**: Library doesn't log/handle task errors - caller's responsibility

## License

MIT License - See LICENSE file for details
