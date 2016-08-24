/*
Copyright 2016 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package etcdkv

import (
	"os"
	"strconv"
	"testing"

	"github.com/gravitational/coordinate/defaults"
	"github.com/gravitational/coordinate/kv/suite"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
	. "gopkg.in/check.v1"
)

func TestETCD(t *testing.T) { TestingT(t) }

type ESuite struct {
	engine *TempEngine
	suite  suite.EngineSuite
}

var _ = Suite(&ESuite{})

func (s *ESuite) SetUpTest(c *C) {
	log.SetOutput(os.Stderr)

	testETCD := os.Getenv(defaults.TestETCD)

	if ok, _ := strconv.ParseBool(testETCD); !ok {
		c.Skip("Skipping test suite for ETCD")
		return
	}

	var err error
	s.engine, err = NewTemp(os.Getenv(defaults.TestETCDConfig))
	c.Assert(err, IsNil)

	s.suite.E = s.engine.Engine
	s.suite.C = s.engine.Clock
}

func (s *ESuite) TearDownTest(c *C) {
	if s.engine != nil {
		err := s.engine.Delete()
		if err != nil {
			log.Error(trace.DebugReport(err))
		}
		c.Assert(err, IsNil)
	}
}

func (s *ESuite) TestKeysCRUD(c *C) {
	s.suite.KeysCRUD(c)
}
