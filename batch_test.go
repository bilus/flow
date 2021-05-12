package flow_test

import (
	"context"
	"testing"
	"time"

	"github.com/bilus/flow"
	"github.com/stretchr/testify/require"
)

type MockStep struct {
	Facts []flow.Fact
}

func (cs *MockStep) Write(_ context.Context, topic flow.Topic, facts ...flow.Fact) error {
	for _, f := range facts {
		cs.Facts = append(cs.Facts, f)
	}
	return nil
}

func (cs *MockStep) Heartbeat(context.Context) {}

func TestWrite_MaxBufferSize(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	topic := "foo"
	ms := MockStep{}
	buf := flow.Batch(topic, 4, time.Millisecond*50)
	buf.Into(&ms)

	f := flow.Fact{}

	require.NoError(buf.Write(ctx, topic, f))
	require.Empty(ms.Facts)
	require.NoError(buf.Write(ctx, topic, f))
	require.NoError(buf.Write(ctx, topic, f))
	require.NoError(buf.Write(ctx, topic, f))
	require.Len(ms.Facts, 4)
	require.NoError(buf.Write(ctx, topic, f))
	require.Len(ms.Facts, 4)

	buf.Flush(ctx)
	require.Len(ms.Facts, 5)
}

func TestWrite_Flush(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	topic := "foo"
	ms := MockStep{}
	buf := flow.Batch(topic, 4, time.Millisecond*50)
	buf.Into(&ms)

	f := flow.Fact{}

	require.NoError(buf.Write(ctx, topic, f))
	require.Empty(ms.Facts)
	require.NoError(buf.Flush(ctx))
	require.Len(ms.Facts, 1)
}

func TestWrite_Topic(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	topic := "foo"
	ms := MockStep{}
	buf := flow.Batch(topic, 4, time.Millisecond*50)
	buf.Into(&ms)

	wrongTopic := "BAR"
	require.NoError(buf.Write(ctx, wrongTopic, flow.Fact{}))
	buf.Flush(ctx)
	require.Empty(ms.Facts)
}

func TestWrite_Heartbeat(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	topic := "foo"
	ms := MockStep{}
	buf := flow.Batch(topic, 4, time.Millisecond*50)
	buf.Into(&ms)

	f := flow.Fact{}

	require.NoError(buf.Write(ctx, topic, f))
	buf.Heartbeat(ctx)
	require.Empty(ms.Facts)
	time.Sleep(time.Millisecond * 100) // Yuck!

	buf.Heartbeat(ctx)
	require.Len(ms.Facts, 1)

	require.NoError(buf.Write(ctx, topic, f))
	require.Len(ms.Facts, 1)
}
