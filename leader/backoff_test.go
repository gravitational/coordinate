package leader

import (
	"time"

	"github.com/cenkalti/backoff"
	. "gopkg.in/check.v1"
)

type BackOffSuite struct {
}

var _ = Suite(&BackOffSuite{})

type TestClock struct {
	i     time.Duration
	start time.Time
}

func (c *TestClock) Now() time.Time {
	t := c.start.Add(c.i)
	c.i += time.Second
	return t
}

func (s *BackOffSuite) TestCountsSteps(c *C) {
	backoff := &CountingBackOff{
		backoff: backoff.NewExponentialBackOff(),
	}

	backoff.NextBackOff()
	backoff.NextBackOff()
	c.Assert(backoff.Tries(), Equals, 2)

	backoff.Reset()
	c.Assert(backoff.Tries(), Equals, 0)
}

func (s *BackOffSuite) TestAlternatesIntervals(c *C) {
	backoff := NewFlippingBackOff(
		backoff.NewConstantBackOff(1*time.Second),
		backoff.NewExponentialBackOff(),
	)

	backoff.NextBackOff()
	backoff.NextBackOff()
	c.Assert(backoff.NextBackOff(), Equals, 1*time.Second)

	backoff.SetFailing(true)
	backoff.NextBackOff()
	c.Assert(backoff.NextBackOff(), Not(Equals), 1*time.Second)

	backoff.SetFailing(false)
	backoff.NextBackOff()
	c.Assert(backoff.NextBackOff(), Equals, 1*time.Second)
}
