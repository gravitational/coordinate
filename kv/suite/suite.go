// package suite contains a storage acceptance test suite that is backend
// implementation independent each storage will use the suite to test itself
package suite

import (
	"time"

	"github.com/gravitational/coordinate/kv"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	. "gopkg.in/check.v1"
)

var now = time.Date(2015, 11, 16, 1, 2, 3, 0, time.UTC)

type EngineSuite struct {
	E kv.Engine
	C clockwork.FakeClock
}

func (s *EngineSuite) KeysCRUD(c *C) {
	val := "hello"
	err := s.E.CreateVal(s.E.Key("a", "b"), val, kv.Forever)
	c.Assert(err, IsNil)
	err = s.E.CreateVal(s.E.Key("a", "b"), val, kv.Forever)
	c.Assert(trace.IsAlreadyExists(err), Equals, true, Commentf("%T", err))
}
