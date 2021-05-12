package flow

import (
	"context"
	"log"
	"sync"
	"time"
)

const HeartbeatInterval = time.Second

func Run(ctx context.Context, runnables ...Runnable) {
	wc := sync.WaitGroup{}
	for _, r := range runnables {
		runnable := r
		wc.Add(1)
		go func() {
			defer wc.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					time.Sleep(HeartbeatInterval)
					runnable.Heartbeat(ctx)
				}
			}
		}()

		wc.Add(1)
		go func() {
			defer wc.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					delay, err := runnable.Run(ctx)
					if err != nil {
						if err == ErrTerminate {
							return
						}
						log.Printf("error while running: %v", err)
					}
					time.Sleep(delay)
				}
			}
		}()
	}
	wc.Wait()
}
