package kafka

import (
	"context"
	"errors"
	"time"
)

var retryDelays = []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}

// Permanent marks a handler/decode failure that must be forwarded without retry.
type Permanent struct{ Err error }

func (e Permanent) Error() string {
	if e.Err == nil {
		return "kafka: permanent failure"
	}
	return e.Err.Error()
}

func (e Permanent) Unwrap() error { return e.Err }

// IsPermanent reports whether an error must bypass retry delays.
func IsPermanent(err error) bool {
	var permanent Permanent
	return errors.As(err, &permanent)
}

// Sleeper makes retry scheduling deterministic in tests.
type Sleeper interface {
	Sleep(context.Context, time.Duration) error
}

type realSleeper struct{}

func (realSleeper) Sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Retry runs the initial attempt then the contract's 2/4/8-second retries.
func Retry(ctx context.Context, sleeper Sleeper, operation func() error) (attempts int, err error) {
	if sleeper == nil {
		sleeper = realSleeper{}
	}
	for attempt := 0; ; attempt++ {
		attempts++
		err = operation()
		if err == nil || IsPermanent(err) || attempt == len(retryDelays) {
			return attempts, err
		}
		if err = sleeper.Sleep(ctx, retryDelays[attempt]); err != nil {
			return attempts, err
		}
	}
}
