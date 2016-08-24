package boltkv

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/gravitational/coordinate/kv"
	"github.com/gravitational/coordinate/kv/suite"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	. "gopkg.in/check.v1"
)

type BSuite struct {
	engine *tempBolt
	suite  suite.EngineSuite
}

var _ = Suite(&BSuite{})

// tempBolt helps to create and destroy ad-hock bolt databases
type tempBolt struct {
	clock  clockwork.FakeClock
	engine *Bolt
	dir    string
}

func (t *tempBolt) Delete() error {
	var errs []error
	if t.engine != nil {
		errs = append(errs, t.engine.Close())
	}
	if t.dir != "" {
		errs = append(errs, os.RemoveAll(t.dir))
	}
	return trace.NewAggregate(errs...)
}

func newTempBolt() (*tempBolt, error) {
	dir, err := ioutil.TempDir("", "gravity-test")
	if err != nil {
		return nil, trace.Wrap(err)
	}
	fakeClock := clockwork.NewFakeClock()
	engine, err := New(Config{
		Clock: fakeClock,
		Path:  filepath.Join(dir, "bolt.db"),
		Codec: &kv.JSONCodec{},
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &tempBolt{
		dir:    dir,
		clock:  fakeClock,
		engine: engine,
	}, nil
}

func (s *BSuite) SetUpTest(c *C) {
	log.SetOutput(os.Stderr)

	var err error
	s.engine, err = newTempBolt()
	c.Assert(err, IsNil)

	s.suite.E = s.engine.engine
	s.suite.C = s.engine.clock
}

func (s *BSuite) TearDownTest(c *C) {
	if s.engine != nil {
		err := s.engine.Delete()
		if err != nil {
			log.Error(trace.DebugReport(err))
		}
		c.Assert(err, IsNil)
	}
}

func (s *BSuite) TestKeysCRUD(c *C) {
	s.suite.KeysCRUD(c)
}
