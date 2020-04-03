package clock

import (
	"time"

	"github.com/cenkalti/backoff"
	"github.com/jonboulle/clockwork"
)

type Clock interface {
	clockwork.Clock
	NewTickerWithBackOff(backoff.BackOff) clockwork.Ticker
}

type FakeClock interface {
	Clock
	// Advance advances the FakeClock to a new point in time, ensuring any existing
	// sleepers are notified appropriately before returning
	Advance(d time.Duration)
	// BlockUntil will block until the FakeClock has the given number of
	// sleepers (callers of Sleep or After)
	BlockUntil(n int)
}

func NewFakeClock(backoffInterval time.Duration) FakeClock {
	return &fakeClock{
		FakeClock: clockwork.NewFakeClock(),
		interval:  backoffInterval,
	}
}

// NewTickerWithBackOff creates a new ticker using the specified backoff interval.
// Falls back to clockwork.FakeClock.NewTicker using the first backoff interval that v produces
func (r *fakeClock) NewTickerWithBackOff(backoff.BackOff) clockwork.Ticker {
	return r.FakeClock.NewTicker(r.interval)
}

type fakeClock struct {
	clockwork.FakeClock
	interval time.Duration
}

func NewRealClock() Clock {
	return &realClock{}
}

type realClock struct{}

func (*realClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (*realClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

func (*realClock) Now() time.Time {
	return time.Now()
}

func (r *realClock) Since(t time.Time) time.Duration {
	return r.Now().Sub(t)
}

func (*realClock) NewTickerWithBackOff(b backoff.BackOff) clockwork.Ticker {
	return &realTickerWithBackOff{backoff.NewTicker(b)}
}

func (*realClock) NewTicker(d time.Duration) clockwork.Ticker {
	return &realTicker{time.NewTicker(d)}
}

type realTickerWithBackOff struct{ *backoff.Ticker }

func (r *realTickerWithBackOff) Chan() <-chan time.Time {
	return r.C
}

type realTicker struct{ *time.Ticker }

func (r *realTicker) Chan() <-chan time.Time {
	return r.C
}
