package flow

import (
	"context"
	"time"
)

type Topic = string

type Fact struct {
	FactID        string    `bigquery:"_factId"`
	TransactionID string    `bigquery:"_transactionId"`
	Data          string    `bigquery:"_data"`
	Timestamp     time.Time `bigquery:"_timestamp"`
}

type Runnable interface {
	Heartbeat(ctx context.Context)
	Run(ctx context.Context) (nextInterval time.Duration, err error)
}

type Step interface {
	Heartbeat(ctx context.Context)
	Write(ctx context.Context, topic Topic, facts ...Fact) error
}

type Sink interface {
	Step
}

type Transform interface {
	Step
	Transform(next Transform) Transform
	Into(next Sink)
}
