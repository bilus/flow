package flow

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

type batchTransform struct {
	mtx          sync.Mutex
	buffer       []Fact
	lastFlush    time.Time
	flushEvery   time.Duration
	maxBatchSize int

	topic Topic
	next  Step
}

func Batch(topic Topic, size int, flushEvery time.Duration) *batchTransform {
	return &batchTransform{
		flushEvery:   flushEvery,
		maxBatchSize: size,
		lastFlush:    time.Now(),

		topic: topic,
	}
}

func (b *batchTransform) Transform(next Transform) Transform {
	b.mtx.Lock()
	b.next = next
	b.mtx.Unlock()
	return next
}

func (s *batchTransform) Into(next Sink) {
	s.next = next
}

func (b *batchTransform) Write(ctx context.Context, topic Topic, facts ...Fact) (err error) {
	b.mtx.Lock()
	for _, f := range facts {
		if topic == b.topic {
			b.buffer = append(b.buffer, f)
		}
	}

	err = b.flush(ctx, b.maxBatchSize)
	b.mtx.Unlock()
	return
}

func (b *batchTransform) Flush(ctx context.Context) (err error) {
	b.mtx.Lock()
	err = b.flush(ctx, 1)
	b.mtx.Unlock()
	return
}

// flush flushes the buffer if it has more than maxSize items.
// When called must be protected by locking the mutex.
func (b *batchTransform) flush(ctx context.Context, maxSize int) (err error) {
	if b.next == nil {
		return errors.New("internal error: missing Into() call for a Batcher")
	}
	if len(b.buffer) >= maxSize {
		log.Printf("Flushing %v fact(s)", len(b.buffer))
		err = b.next.Write(ctx, b.topic, b.buffer...)
		if err == nil {
			b.buffer = nil
			b.lastFlush = time.Now()
		} else {
			log.Printf("Error writing to next step: %v", err)
		}
	}
	return
}

func (b *batchTransform) Heartbeat(ctx context.Context) {
	if b.next == nil {
		return
	}
	b.next.Heartbeat(ctx)

	b.mtx.Lock()
	if time.Since(b.lastFlush).Milliseconds() >= b.flushEvery.Milliseconds() {
		b.flush(ctx, 1)
	}
	b.mtx.Unlock()
}
