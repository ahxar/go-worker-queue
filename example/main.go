package main

import (
	"context"
	"fmt"
	"log"
	"time"

	workerqueue "github.com/safar/go-worker-queue"
)

func main() {
	fmt.Println("=== Worker Queue Example ===")
	fmt.Println()

	// Create a worker queue with 3 workers and queue capacity of 5
	wq := workerqueue.New(3, 5)

	// Start the workers
	if err := wq.Start(); err != nil {
		log.Fatalf("Failed to start worker queue: %v", err)
	}

	fmt.Println("Worker queue started with 3 workers and capacity of 5")
	fmt.Println()

	// Example 1: Basic task execution
	fmt.Println("--- Example 1: Basic Task Execution ---")
	for i := 1; i <= 5; i++ {
		taskID := i
		task := func(ctx context.Context) error {
			fmt.Printf("Task %d: Starting execution\n", taskID)
			time.Sleep(500 * time.Millisecond)
			fmt.Printf("Task %d: Completed\n", taskID)
			return nil
		}

		if err := wq.Submit(context.Background(), task); err != nil {
			log.Printf("Failed to submit task %d: %v", taskID, err)
		}
	}

	// Wait for tasks to complete
	time.Sleep(2 * time.Second)
	fmt.Println()

	// Example 2: Backpressure demonstration
	fmt.Println("--- Example 2: Backpressure Demonstration ---")
	fmt.Println("Submitting 10 tasks (more than queue capacity)...")

	// Submit tasks rapidly
	for i := 1; i <= 10; i++ {
		taskID := i
		task := func(ctx context.Context) error {
			time.Sleep(300 * time.Millisecond)
			return nil
		}

		// Use context with timeout to demonstrate backpressure
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := wq.Submit(ctx, task); err != nil {
			fmt.Printf("Task %d: Submit blocked/failed: %v\n", taskID, err)
			cancel()
			continue
		}
		cancel()
		fmt.Printf("Task %d: Submitted successfully\n", taskID)
	}

	fmt.Println()

	// Example 3: Context cancellation
	fmt.Println("--- Example 3: Context Cancellation ---")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	longTask := func(taskCtx context.Context) error {
		fmt.Println("Long-running task: Started")
		select {
		case <-time.After(5 * time.Second):
			fmt.Println("Long-running task: Completed normally")
			return nil
		case <-taskCtx.Done():
			fmt.Println("Long-running task: Cancelled by context")
			return taskCtx.Err()
		}
	}

	if err := wq.Submit(ctx, longTask); err != nil {
		fmt.Printf("Failed to submit long task: %v\n", err)
	}

	time.Sleep(1 * time.Second)
	fmt.Println()

	// Example 4: Graceful shutdown
	fmt.Println("--- Example 4: Graceful Shutdown ---")

	// Submit final tasks
	for i := 1; i <= 3; i++ {
		taskID := i
		task := func(ctx context.Context) error {
			fmt.Printf("Final task %d: Processing...\n", taskID)
			time.Sleep(500 * time.Millisecond)
			fmt.Printf("Final task %d: Done\n", taskID)
			return nil
		}

		if err := wq.Submit(context.Background(), task); err != nil {
			log.Printf("Failed to submit final task %d: %v", taskID, err)
		}
	}

	fmt.Println("Initiating graceful shutdown (waiting for tasks to complete)...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := wq.Shutdown(shutdownCtx); err != nil {
		log.Printf("Shutdown error: %v", err)
	} else {
		fmt.Println("Graceful shutdown completed successfully")
	}

	fmt.Println("\n=== Example Complete ===")
}
