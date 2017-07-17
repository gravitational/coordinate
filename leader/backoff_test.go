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

func (s *BackOffSuite) TestBackOff(c *C) {
	backoff := &CountingBackOff{
		backoff: backoff.NewExponentialBackOff(),
	}

	backoff.NextBackOff()
	backoff.NextBackOff()
	c.Assert(backoff.NumTries(), Equals, 2)

	backoff.Reset()
	c.Assert(backoff.NumTries(), Equals, 0)
}
