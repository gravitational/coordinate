package leader

import (
	"time"

	"github.com/cenkalti/backoff"
)

func NewCountingBackOff(backOff backoff.BackOff) *CountingBackOff {
	return &CountingBackOff{
		backoff: backOff,
	}
}

// CountingBackOff is an exponential backoff that
// counts the number of backoff steps
type CountingBackOff struct {
	backoff backoff.BackOff

	tries int
}

// NextBackOff returns the duration of the next backoff interval
func (f *CountingBackOff) NextBackOff() time.Duration {
	f.tries++
	return f.backoff.NextBackOff()
}

// Reset resets the number of tries on this backoff counter to zero
func (f *CountingBackOff) Reset() {
	f.tries = 0
	f.backoff.Reset()
}

// NumTries returns the number of attempts on this backoff counter
func (f *CountingBackOff) NumTries() int {
	return f.tries
}
