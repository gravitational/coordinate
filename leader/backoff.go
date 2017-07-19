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

// CountingBackOff is a backoff that counts the number of taken steps
type CountingBackOff struct {
	backoff backoff.BackOff

	// tries describes the number of backoff steps taken
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

// Tries returns the number of steps taken along this backoff interval
func (f *CountingBackOff) Tries() int {
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

// SetFailing resets the failing state to failing.
// If failing == false, the failing backoff interval is reset.
func (r *FlippingBackOff) SetFailing(failing bool) {
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

// FlippingBackOff provides a backoff using two backoff implementations.
// The backoff implementation can be switched by calling SetFailing with
// appropriate value.
//
// This can be useful in conjunction with the backoff.Ticker to provide a custom
// loop that can dynamically switch between backoff implementations depending
// on the state of an operation (e.g. healthy vs having transient errors).
type FlippingBackOff struct {
	// regular specifies the backoff implementation to use
	// for non-error conditions (SetFailing(false))
	regular backoff.BackOff
	// failing specifies the backoff implementation to use
	// for error conditions (SetFailing(true))
	failing backoff.BackOff

	// isFailing indicates if the failing backoff implementation is in effect
	isFailing bool
}

// NewUnlimitedExponentialBackOff returns a new exponential backoff interval
// w/o time limit
func NewUnlimitedExponentialBackOff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0
	return b
}
