package flow

import (
	"context"
	"sync"
	"time"
)

type batchByTopicTransform struct {
	mtx sync.Mutex

	targets      map[Topic]*batchTransform // Can be generalized by why bother
	maxBatchSize int
	flushEvery   time.Duration

	next Step
}

func BatchByTopic(maxBatchSize int, flushEvery time.Duration) *batchByTopicTransform {
	return &batchByTopicTransform{
		targets:      make(map[Topic]*batchTransform),
		maxBatchSize: maxBatchSize,
		flushEvery:   flushEvery,
	}
}

func (b *batchByTopicTransform) Transform(next Transform) Transform {
	b.next = next
	return next
}

func (s *batchByTopicTransform) Into(next Sink) {
	s.next = next
}

func (b *batchByTopicTransform) Write(ctx context.Context, topic Topic, facts ...Fact) (err error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	target, ok := b.targets[topic]
	if !ok {
		target = Batch(topic, b.maxBatchSize, b.flushEvery)
		target.Into(b.next)
		b.targets[topic] = target
	}

	for _, target := range b.targets {
		// Batchers will filter by topic. If this changes, we'll need more sophisticated logic here.
		if err := target.Write(ctx, topic, facts...); err != nil {
			return err // TODO: Do we want to fail?
		}
	}

	return
}

func (b *batchByTopicTransform) Heartbeat(ctx context.Context) {
	b.mtx.Lock()

	for _, target := range b.targets {
		target.Heartbeat(ctx)
	}

	b.mtx.Unlock()
}
