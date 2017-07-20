package leader

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gravitational/coordinate/config"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/client"
	"github.com/pborman/uuid"
	. "gopkg.in/check.v1"
)

func TestLeader(t *testing.T) { TestingT(t) }

type LeaderSuite struct {
	nodes []string
}

var _ = Suite(&LeaderSuite{})

func (s *LeaderSuite) SetUpSuite(c *C) {
	log.SetOutput(os.Stderr)
	nodesString := os.Getenv("COORDINATE_TEST_ETCD_NODES")
	if nodesString == "" {
		// Skips the entire suite
		c.Skip("This test requires etcd, provide comma separated nodes in COORDINATE_TEST_ETCD_NODES environment variable")
		return
	}
	s.nodes = strings.Split(nodesString, ",")
}

func (s *LeaderSuite) newClient(c *C) *Client {
	clt, err := NewClient(
		Config{
			ETCD: &config.Config{
				Endpoints:               s.nodes,
				HeaderTimeoutPerRequest: 100 * time.Millisecond,
			},
		},
	)
	c.Assert(err, IsNil)
	return clt
}

func (s *LeaderSuite) closeClient(c *C, clt *Client) {
	err := clt.Close()
	c.Assert(err, IsNil)
}

func (s *LeaderSuite) TestLeaderElectSingle(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	clt.AddWatchCallback(context.TODO(), key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})
	clt.AddVoter(context.TODO(), key, "node1", time.Second)

	select {
	case val := <-changeC:
		c.Assert(val, Equals, "node1")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}
}

func (s *LeaderSuite) TestReceiveExistingValue(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	clt.AddWatchCallback(context.TODO(), key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})
	api := client.NewKeysAPI(clt.client)
	_, err := api.Set(context.TODO(), key, "first", nil)
	c.Assert(err, IsNil)

	select {
	case val := <-changeC:
		c.Assert(val, Equals, "first")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}
}

func (s *LeaderSuite) TestLeaderTakeover(c *C) {
	clta := s.newClient(c)

	cltb := s.newClient(c)
	defer s.closeClient(c, cltb)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	cltb.AddWatchCallback(context.TODO(), key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})
	clta.AddVoter(context.TODO(), key, "voter a", time.Second)

	// make sure we've elected voter a
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter a")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	// add voter b to the equation
	cltb.AddVoter(context.TODO(), key, "voter b", time.Second)

	// now, shut down voter a
	c.Assert(clta.Close(), IsNil)
	// in a second, we should see the leader has changed
	time.Sleep(time.Second)

	// make sure we've elected voter b
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter b")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}
}

func (s *LeaderSuite) TestLeaderReelectionWithSingleClient(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	clt.AddWatchCallback(context.TODO(), key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})
	ctx, cancel := context.WithCancel(context.TODO())
	err := clt.AddVoter(ctx, key, "voter a", time.Second)
	c.Assert(err, IsNil)

	// make sure we've elected voter a
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter a")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	// add another voter
	clt.AddVoter(context.TODO(), key, "voter b", time.Second)

	// now, shut down voter a
	cancel()
	// in a second, we should see the leader has changed
	time.Sleep(time.Second)

	// make sure we've elected voter b
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter b")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}
}

// Make sure leader extends lease in time
func (s *LeaderSuite) TestLeaderExtendLease(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())
	clt.AddVoter(context.TODO(), key, "voter a", time.Second)
	time.Sleep(900 * time.Millisecond)

	api := client.NewKeysAPI(clt.client)
	re, err := api.Get(context.TODO(), key, nil)
	c.Assert(err, IsNil)
	expiresIn := re.Node.Expiration.Sub(time.Now())
	maxTTL := 500 * time.Millisecond
	c.Assert(expiresIn > maxTTL, Equals, true, Commentf("%v > %v", expiresIn, maxTTL))
}

// Make sure we can recover from getting an expired index from our watch
func (s *LeaderSuite) TestHandleLostIndex(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/index/%v", uuid.New())
	kapi := client.NewKeysAPI(clt.client)

	changeC := make(chan string)
	clt.AddWatchCallback(context.TODO(), key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})

	last := ""
	log.Info("setting our key 1100 times")
	for i := 0; i < 1100; i++ {
		val := fmt.Sprintf("%v", uuid.New())
		kapi.Set(context.TODO(), key, val, nil)
		last = val
	}

	for {
		select {
		case val := <-changeC:
			if val == last {
				log.Infof("got expected final value from watch")
				return
			}
		case <-time.After(1 * time.Second):
			c.Fatalf("never got anticipated last value from watch")
		}
	}
}

func (s *LeaderSuite) TestStepDown(c *C) {
	clta := s.newClient(c)
	defer s.closeClient(c, clta)

	cltb := s.newClient(c)
	defer s.closeClient(c, cltb)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	cltb.AddWatchCallback(context.TODO(), key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})

	// add voter a
	clta.AddVoter(context.TODO(), key, "voter a", time.Second)

	// make sure we've elected voter a
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter a")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	// add voter b
	cltb.AddVoter(context.TODO(), key, "voter b", time.Second)

	// tell voter a to step down and wait for the next term
	clta.StepDown()
	time.Sleep(time.Second)

	// make sure voter b is elected
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter b")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}
}
