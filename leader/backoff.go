package leader

import (
	"time"

	"github.com/cenkalti/backoff"
)

// NewCountingBackOff returns a new instance of the CountingBackOff
// with the specified backoff as implementation
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

// Reset resets both the number of steps and the backoff interval to zero
func (f *CountingBackOff) Reset() {
	f.tries = 0
	f.backoff.Reset()
}

// NumTries returns the number of steps taken along this backoff interval
func (f *CountingBackOff) NumTries() int {
	return f.tries
}

// NewFlippingBackOff returns a new instance of the FlippingBackOff
// using regular and failing as backoff implementations
func NewFlippingBackOff(regular, failing backoff.BackOff) *FlippingBackOff {
	return &FlippingBackOff{
		regular: regular,
		failing: failing,
	}
}

// Failing resets the failing state to failing.
// If failing == false, the failing backoff interval is reset.
func (r *FlippingBackOff) Failing(failing bool) {
	r.isFailing = failing
	if !failing {
		r.failing.Reset()
	}
}

// NextBackOff returns the duration of the next backoff interval
func (r *FlippingBackOff) NextBackOff() time.Duration {
	if r.isFailing {
		return r.failing.NextBackOff()
	} else {
		return r.regular.NextBackOff()
	}
}

// Reset resets the underlying backoff intervals
func (r *FlippingBackOff) Reset() {
	r.regular.Reset()
	r.failing.Reset()
}

// FlippingBackOff provides a backoff using two backoff implementations
// it alternates between.
// First implementation is used to provide regular notifications,
// while the second implementation is used to handle errors.
//
// This can be used in conjunction with the backoff.Ticker to provide a custom
// loop that can dynamically switch between backoff implementations
// depending on state of an operation (e.g. healthy vs experiencing transient errors).
type FlippingBackOff struct {
	regular, failing backoff.BackOff
	isFailing        bool
}
