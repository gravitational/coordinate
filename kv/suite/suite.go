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

func (s *EngineSuite) DirsCRUD(c *C) {
	val := "hello"
	key := s.E.Key("a", "b")
	err := s.E.CreateVal(key, val, kv.Forever)
	c.Assert(err, IsNil)

	isDir, err := s.E.IsDir(key)
	c.Assert(err, IsNil)
	c.Assert(isDir, Equals, false)

	isDir, err = s.E.IsDir(s.E.Key("a"))
	c.Assert(err, IsNil)
	c.Assert(isDir, Equals, true)

	_, err = s.E.IsDir(s.E.Key("c"))
	c.Assert(trace.IsNotFound(err), Equals, true, Commentf("%T", err))
}

func (s *EngineSuite) DeleteIf(c *C) {
	val := "hello"
	key := s.E.Key("a", "b")
	err := s.E.CreateVal(key, val, kv.Forever)
	c.Assert(err, IsNil)

	err = s.E.DeleteKeyIf(key, "hello1")
	c.Assert(trace.IsCompareFailed(err), Equals, true, Commentf("%T", err))

	err = s.E.DeleteKeyIf(key, "hello")
	c.Assert(err, IsNil)
}
