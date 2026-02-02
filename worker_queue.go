package workerqueue

import (
	"context"
	"fmt"
	"sync"
)

// Task represents a unit of work to be executed by a worker.
// It accepts a context for cancellation and returns an error if execution fails.
type Task func(ctx context.Context) error

// WorkerQueue manages a fixed pool of worker goroutines that process tasks from a bounded queue.
type WorkerQueue struct {
	workers int
	tasks   chan Task
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
	mu      sync.Mutex
}

// New creates a new WorkerQueue with the specified number of workers and queue capacity.
// workers: number of concurrent worker goroutines
// queueSize: maximum number of tasks that can be queued (provides backpressure)
func New(workers, queueSize int) *WorkerQueue {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerQueue{
		workers: workers,
		tasks:   make(chan Task, queueSize),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start launches the worker goroutines. Must be called before Submit.
// Returns an error if already started.
func (wq *WorkerQueue) Start() error {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.started {
		return fmt.Errorf("worker queue already started")
	}

	wq.started = true

	for i := 0; i < wq.workers; i++ {
		wq.wg.Add(1)
		go wq.worker()
	}

	return nil
}

// worker is the main loop for each worker goroutine.
// It continuously polls the task channel and executes tasks until:
// - The task channel is closed, OR
// - The context is cancelled
func (wq *WorkerQueue) worker() {
	defer wq.wg.Done()

	for {
		select {
		case <-wq.ctx.Done():
			// Context cancelled, exit worker
			return
		case task, ok := <-wq.tasks:
			if !ok {
				// Channel closed, exit worker
				return
			}
			// Execute the task with the queue's context
			// Errors are silently ignored - caller should handle logging if needed
			_ = task(wq.ctx)
		}
	}
}

// Submit adds a task to the queue for execution.
// It blocks if the queue is full (backpressure mechanism).
// The provided context allows the caller to cancel/timeout the submission.
// Returns an error if:
// - The context is cancelled/times out
// - The worker queue has been stopped
func (wq *WorkerQueue) Submit(ctx context.Context, task Task) error {
	wq.mu.Lock()
	if !wq.started {
		wq.mu.Unlock()
		return fmt.Errorf("worker queue not started")
	}
	wq.mu.Unlock()

	// Check if worker queue context is already cancelled
	// This prevents race condition where channel send might be chosen over cancelled context
	select {
	case <-wq.ctx.Done():
		return fmt.Errorf("worker queue stopped")
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wq.ctx.Done():
		return fmt.Errorf("worker queue stopped")
	case wq.tasks <- task:
		return nil
	}
}

// Shutdown performs a graceful shutdown:
// 1. Stops accepting new tasks
// 2. Closes the task channel
// 3. Waits for all queued tasks to complete
// 4. Waits for all workers to exit
//
// The provided context sets a deadline for the shutdown.
// Returns an error if the context expires before all workers finish.
func (wq *WorkerQueue) Shutdown(ctx context.Context) error {
	wq.mu.Lock()
	if !wq.started {
		wq.mu.Unlock()
		return fmt.Errorf("worker queue not started")
	}
	wq.mu.Unlock()

	// Close task channel to signal no more tasks
	close(wq.tasks)

	// Wait for workers to finish with context deadline
	done := make(chan struct{})
	go func() {
		wq.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All workers finished successfully
		return nil
	case <-ctx.Done():
		// Context expired, force cancel and wait
		wq.cancel()
		<-done
		return ctx.Err()
	}
}

// Stop immediately stops the worker queue:
// 1. Cancels the context (signals workers to stop)
// 2. Waits for all workers to exit
// 3. Any queued tasks that haven't started will not be executed
//
// This is a forceful shutdown - use Shutdown() for graceful termination.
func (wq *WorkerQueue) Stop() {
	wq.mu.Lock()
	if !wq.started {
		wq.mu.Unlock()
		return
	}
	wq.mu.Unlock()

	// Cancel context to signal workers
	wq.cancel()

	// Wait for all workers to exit
	wq.wg.Wait()
}
