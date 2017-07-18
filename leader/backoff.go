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

// Reset resets the number of tries on this backoff counter to zero
func (f *CountingBackOff) Reset() {
	f.tries = 0
	f.backoff.Reset()
}

// NumTries returns the number of attempts on this backoff counter
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
// If failing == false, the failing backoff is reset.
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

// Reset resets the underlying backoffs
func (r *FlippingBackOff) Reset() {
	r.regular.Reset()
	r.failing.Reset()
}

// FlippingBackOff provides a backoff using two backoff implementations.
// First implementation is used to provide regular spaced notifications,
// while the second implementation is used to provide notifications if the
// status has been set to failing.
// This can be used in conjunction with the backoff.Ticker to provide a custom
// loop that can dynamically switch to an alternative backoff implementation
// in case of errors.
type FlippingBackOff struct {
	regular, failing backoff.BackOff
	isFailing        bool
}
