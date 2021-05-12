package flow

import (
	"context"
	"errors"
	"time"
)

// ErrTerminate should be returned from Run to stop running the source.
var ErrTerminate = errors.New("source terminated")

type Source struct {
	Next Step
}

func (s *Source) Transform(next Transform) Transform {
	s.Next = next
	return next
}

func (s *Source) Into(next Sink) {
	s.Next = next
}

func (s *Source) Heartbeat(ctx context.Context) {
	if s.Next != nil {
		s.Next.Heartbeat(ctx)
	}
}

func (s *Source) Run(ctx context.Context) (time.Duration, error) {
	return time.Minute * 60, nil
}
