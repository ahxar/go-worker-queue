package workerqueue

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBasicExecution(t *testing.T) {
	wq := New(2, 10)
	if err := wq.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer wq.Stop()

	var counter atomic.Int32
	task := func(ctx context.Context) error {
		counter.Add(1)
		return nil
	}

	// Submit 5 tasks
	for range 5 {
		if err := wq.Submit(context.Background(), task); err != nil {
			t.Fatalf("failed to submit task: %v", err)
		}
	}

	// Wait a bit for tasks to complete
	time.Sleep(100 * time.Millisecond)

	if got := counter.Load(); got != 5 {
		t.Errorf("expected 5 tasks executed, got %d", got)
	}
}

func TestBackpressure(t *testing.T) {
	// Create queue with 1 worker and capacity of 2
	wq := New(1, 2)
	if err := wq.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer wq.Stop()

	// Block the worker with a long-running task
	blockCh := make(chan struct{})
	blockTask := func(ctx context.Context) error {
		<-blockCh
		return nil
	}

	// Submit blocking task
	if err := wq.Submit(context.Background(), blockTask); err != nil {
		t.Fatalf("failed to submit blocking task: %v", err)
	}

	// Fill the queue (capacity 2, one is being processed)
	quickTask := func(ctx context.Context) error { return nil }
	if err := wq.Submit(context.Background(), quickTask); err != nil {
		t.Fatalf("failed to submit task 1: %v", err)
	}
	if err := wq.Submit(context.Background(), quickTask); err != nil {
		t.Fatalf("failed to submit task 2: %v", err)
	}

	// Next submit should block - use context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := wq.Submit(ctx, quickTask)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected deadline exceeded due to backpressure, got: %v", err)
	}

	// Unblock and verify tasks complete
	close(blockCh)
	time.Sleep(100 * time.Millisecond)
}

func TestContextCancellation(t *testing.T) {
	wq := New(2, 10)
	if err := wq.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	var started atomic.Int32
	var cancelled atomic.Int32

	task := func(ctx context.Context) error {
		started.Add(1)
		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Done():
			cancelled.Add(1)
			return ctx.Err()
		}
	}

	// Submit tasks
	for range 5 {
		if err := wq.Submit(context.Background(), task); err != nil {
			t.Fatalf("failed to submit task: %v", err)
		}
	}

	// Give tasks time to start
	time.Sleep(50 * time.Millisecond)

	// Stop immediately (cancels context)
	wq.Stop()

	// Verify some tasks were cancelled
	if started.Load() == 0 {
		t.Error("no tasks started")
	}
	if cancelled.Load() == 0 {
		t.Error("expected some tasks to be cancelled")
	}
}

func TestGracefulShutdown(t *testing.T) {
	wq := New(2, 10)
	if err := wq.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	var counter atomic.Int32
	task := func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		counter.Add(1)
		return nil
	}

	// Submit 5 tasks
	taskCount := 5
	for range taskCount {
		if err := wq.Submit(context.Background(), task); err != nil {
			t.Fatalf("failed to submit task: %v", err)
		}
	}

	// Graceful shutdown with sufficient timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := wq.Shutdown(ctx); err != nil {
		t.Errorf("shutdown failed: %v", err)
	}

	// All tasks should complete
	if got := counter.Load(); got != int32(taskCount) {
		t.Errorf("expected %d tasks completed, got %d", taskCount, got)
	}
}

func TestShutdownTimeout(t *testing.T) {
	wq := New(1, 10)
	if err := wq.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Submit a long-running task
	longTask := func(ctx context.Context) error {
		time.Sleep(2 * time.Second)
		return nil
	}

	if err := wq.Submit(context.Background(), longTask); err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	// Shutdown with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := wq.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected deadline exceeded, got: %v", err)
	}
}

func TestNoGoroutineLeaks(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	wq := New(5, 20)
	if err := wq.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Submit some tasks
	task := func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	for range 10 {
		if err := wq.Submit(context.Background(), task); err != nil {
			t.Fatalf("failed to submit task: %v", err)
		}
	}

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := wq.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Wait a bit for goroutines to clean up
	time.Sleep(100 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	if finalGoroutines > initialGoroutines+1 { // +1 for test goroutine tolerance
		t.Errorf("goroutine leak detected: initial=%d, final=%d", initialGoroutines, finalGoroutines)
	}
}

func TestSubmitBeforeStart(t *testing.T) {
	wq := New(2, 10)

	task := func(ctx context.Context) error { return nil }
	err := wq.Submit(context.Background(), task)

	if err == nil {
		t.Error("expected error when submitting before start")
	}
}

func TestDoubleStart(t *testing.T) {
	wq := New(2, 10)
	if err := wq.Start(); err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	defer wq.Stop()

	err := wq.Start()
	if err == nil {
		t.Error("expected error on double start")
	}
}

func TestSubmitAfterStop(t *testing.T) {
	wq := New(2, 10)
	if err := wq.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	wq.Stop()

	// Small delay to ensure context cancellation propagates
	time.Sleep(10 * time.Millisecond)

	task := func(ctx context.Context) error { return nil }
	err := wq.Submit(context.Background(), task)

	if err == nil {
		t.Error("expected error when submitting after stop")
	}
}

func TestConcurrentSubmit(t *testing.T) {
	wq := New(5, 100)
	if err := wq.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	defer wq.Stop()

	var counter atomic.Int32
	task := func(ctx context.Context) error {
		counter.Add(1)
		return nil
	}

	// Concurrent submissions
	var wg sync.WaitGroup
	submitters := 10
	tasksPerSubmitter := 10

	for range submitters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < tasksPerSubmitter; j++ {
				if err := wq.Submit(context.Background(), task); err != nil {
					t.Errorf("submit failed: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	// Wait for tasks to complete
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := wq.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	expected := int32(submitters * tasksPerSubmitter)
	if got := counter.Load(); got != expected {
		t.Errorf("expected %d tasks executed, got %d", expected, got)
	}
}

// Benchmarks

func BenchmarkSubmit(b *testing.B) {
	wq := New(4, 1000)
	if err := wq.Start(); err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer wq.Stop()

	task := func(ctx context.Context) error { return nil }
	ctx := context.Background()

	for b.Loop() {
		if err := wq.Submit(ctx, task); err != nil {
			b.Fatalf("submit failed: %v", err)
		}
	}
	b.StopTimer()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = wq.Shutdown(shutdownCtx)
}

func BenchmarkWorkerThroughput(b *testing.B) {
	wq := New(runtime.NumCPU(), 10000)
	if err := wq.Start(); err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer wq.Stop()

	var counter atomic.Int32
	task := func(ctx context.Context) error {
		counter.Add(1)
		return nil
	}

	ctx := context.Background()

	for b.Loop() {
		if err := wq.Submit(ctx, task); err != nil {
			b.Fatalf("submit failed: %v", err)
		}
	}

	b.StopTimer()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = wq.Shutdown(shutdownCtx)
}

func BenchmarkConcurrentSubmit(b *testing.B) {
	wq := New(runtime.NumCPU(), 10000)
	if err := wq.Start(); err != nil {
		b.Fatalf("failed to start: %v", err)
	}
	defer wq.Stop()

	task := func(ctx context.Context) error { return nil }
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := wq.Submit(ctx, task); err != nil {
				b.Fatalf("submit failed: %v", err)
			}
		}
	})
	b.StopTimer()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = wq.Shutdown(shutdownCtx)
}
