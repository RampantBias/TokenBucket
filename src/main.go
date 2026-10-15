package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	WorkerCount = 4
)

var WorkerWaits = [WorkerCount]float32{0.5, 1.0, 1.25, 2.5}
var Bucket *TokenBucket
var SuccessCount atomic.Int64
var FailureCount atomic.Int64

type TokenBucket struct {
	rw           *sync.Mutex
	CurrentCount int
	MaxCount     int
	RefillRate   int // per second
}

func (bucket *TokenBucket) UpdateCount(add int) bool {
	bucket.rw.Lock()
	defer bucket.rw.Unlock()

	// Check for no remaining tokens
	if add < 0 && bucket.CurrentCount == 0 {
		return false
	}

	if add > 0 {
		// Check for max bucket capacity
		if bucket.CurrentCount == bucket.MaxCount {
			return false
		}
		bucket.CurrentCount += add

		// Cap max token count, don't fail
		if bucket.CurrentCount > bucket.MaxCount {
			bucket.CurrentCount = bucket.MaxCount
		}
	} else {
		bucket.CurrentCount += add
	}

	return true
}

func (bucket *TokenBucket) refillWorker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			// The shared deadline passed or context was canceled
			log.Printf("Refill Worker stopping: %v\n", ctx.Err())
			return
		case <-time.After(1 * time.Second):
			// prevents case where cancellation and sleep conclude together
			if ctx.Err() != nil {
				return
			}
			log.Printf("Refill worker making request:")
			if bucket.UpdateCount(bucket.RefillRate) {
				log.Print("Refill Worker succeeded")
			} else {
				log.Print("Refill Worker failed")
			}
		}
	}
}

func requestWorker(ctx context.Context, id int, waitTime time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			// The shared deadline passed or context was canceled
			log.Printf("Worker %d stopping: %v\n", id, ctx.Err())
			return
		case <-time.After(waitTime):
			// prevents case where cancellation and sleep conclude together
			if ctx.Err() != nil {
				return
			}
			log.Printf("Worker %d making request:", id)
			if Bucket.UpdateCount(-1) {
				log.Printf("Worker %d succeeded", id)
				SuccessCount.Add(1)
			} else {
				log.Printf("Worker %d failed", id)
				FailureCount.Add(1)
			}
		}
	}
}

func main() {
	deadline := time.Now().Add(1 * time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	// Update log to write microseconds
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// Create Bucket
	Bucket = &TokenBucket{
		rw:           &sync.Mutex{},
		CurrentCount: 5,
		MaxCount:     5,
		RefillRate:   1,
	}

	// Create refill worker
	var wg sync.WaitGroup
	wg.Add(1)
	go Bucket.refillWorker(ctx, &wg)

	// Create request workers
	for i := 1; i <= WorkerCount; i++ {
		wg.Add(1)
		go requestWorker(ctx, i, time.Duration(float64(WorkerWaits[i-1])*float64(time.Second)), &wg)
	}

	wg.Wait()
	log.Print("All workers finished")
	log.Printf("Success Count: %d | Failure Count: %d | Current Bucket Count: %d", SuccessCount.Load(), FailureCount.Load(), Bucket.CurrentCount)
}
