package leader

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gravitational/coordinate/config"

	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client"
	. "gopkg.in/check.v1"
)

func TestLeader(t *testing.T) { TestingT(t) }

type LeaderSuite struct {
	nodes []string
}

var _ = Suite(&LeaderSuite{})

func (s *LeaderSuite) SetUpSuite(c *C) {
	logrus.SetOutput(os.Stderr)
	if testing.Verbose() {
		logrus.SetLevel(logrus.DebugLevel)
	}
	nodesString := os.Getenv("COORDINATE_TEST_ETCD_NODES")
	if nodesString == "" {
		// Skips the entire suite
		c.Skip("This test requires etcd, provide comma separated nodes in COORDINATE_TEST_ETCD_NODES environment variable")
		return
	}
	s.nodes = strings.Split(nodesString, ",")
}

func (s *LeaderSuite) newClient(c *C) *Client {
	etcdConfig := &config.Config{
		Endpoints:               s.nodes,
		HeaderTimeoutPerRequest: 5 * time.Second,
	}
	etcdClient, err := etcdConfig.NewClient()
	c.Assert(err, IsNil)
	clt, err := NewClient(Config{Client: etcdClient})
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
	clt.AddWatchCallback(key, receiver(changeC))
	clt.AddVoter(context.TODO(), key, "node1", time.Second)

	select {
	case val := <-changeC:
		c.Assert(val, Equals, "node1")
	case <-time.After(time.Second):
		c.Fatal("Timeout waiting for event.")
	}
}

func (s *LeaderSuite) TestReceiveExistingValue(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	clt.AddWatchCallback(key, receiver(changeC))
	api := client.NewKeysAPI(clt.Client)
	_, err := api.Set(context.TODO(), key, "first", nil)
	c.Assert(err, IsNil)

	select {
	case val := <-changeC:
		c.Assert(val, Equals, "first")
	case <-time.After(time.Second):
		c.Fatal("Timeout waiting for event.")
	}
}

func (s *LeaderSuite) TestLeaderTakeover(c *C) {
	clta := s.newClient(c)
	defer s.closeClient(c, clta)
	cltb := s.newClient(c)
	defer s.closeClient(c, cltb)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string, 2)
	cltb.AddWatchCallback(key, receiver(changeC))
	clta.AddVoter(context.TODO(), key, "voter a", 1*time.Second)

	// make sure 'voter a' was elected
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter a")
	case <-time.After(time.Second):
		c.Fatal("Timeout waiting for event.")
	}

	// add voter b to the election process
	cltb.AddVoter(context.TODO(), key, "voter b", 1*time.Second)

	// now, shut down 'voter a"
	c.Assert(clta.Close(), IsNil)

	// wait for leader to change
	time.Sleep(2 * time.Second)

	// make sure 'voter b' was elected
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter b")
	case <-time.After(time.Second):
		c.Fatal("Timeout waiting for event.")
	}
}

func (s *LeaderSuite) TestRemoveVoterIsIdempotent(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())
	clt.RemoveVoter(context.Background(), key, "voter", time.Second)
}

func (s *LeaderSuite) TestLeaderReelection(c *C) {
	const term = 1 * time.Second
	clt1 := s.newClient(c)
	clt2 := s.newClient(c)
	defer func() {
		s.closeClient(c, clt1)
		s.closeClient(c, clt2)
	}()

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string, 2)
	clt1.AddWatchCallback(key, receiver(changeC))
	clt1.AddVoter(context.Background(), key, "voter a", term)

	// make sure we've elected voter a
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter a")
	case <-time.After(term):
		c.Fatal("Timeout waiting for event.")
	}

	// add another voter
	clt2.AddVoter(context.Background(), key, "voter b", term)

	// now, shut down voter a
	clt1.RemoveVoter(context.Background(), key, "voter a", term)
	kapi := client.NewKeysAPI(clt1.Client)
	// forcibly remove the key to simulate lease expiration
	_, err := kapi.Delete(context.TODO(), key, &client.DeleteOptions{})
	c.Assert(err, IsNil)

	// in a second, we should see the leader change
	time.Sleep(term)

	// make sure we've elected voter b
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter b")
	case <-time.After(term):
		c.Fatal("Timeout waiting for event.")
	}
}

// Make sure leader extends lease in time
func (s *LeaderSuite) TestLeaderExtendLease(c *C) {
	const (
		term   = 1 * time.Second
		maxTTL = term / 2
	)
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	updateC := make(chan *client.Response, 1)
	clt.updateLeaseC = updateC
	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())
	clt.AddVoter(context.TODO(), key, "voter a", term)

	select {
	case node := <-updateC:
		c.Logf("Extended lease with expiration: %v.", node.Node.Expiration)
	case <-time.After(1 * time.Second):
		c.Fatal("Timeout waiting for lease update.")
	}
}

// Make sure we can recover from getting an expired index from our watch
func (s *LeaderSuite) TestHandleLostIndex(c *C) {
	const numKeys = 100
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/index/%v", uuid.New())
	kapi := client.NewKeysAPI(clt.Client)

	// Buffered to keep the last item
	changeC := make(chan string, 1)
	clt.AddWatchCallback(key, droppingReceiver(changeC))

	last := ""
	c.Logf("Set key %v times.", numKeys)
	for i := 0; i < numKeys; i++ {
		val := strconv.Itoa(i)
		kapi.Set(context.Background(), key, val, nil)
		last = val
	}

	for {
		select {
		case val := <-changeC:
			log.Infof("got value: %s last: %s", val, last)
			if val == last {
				c.Logf("Got expected final value from watch.")
				return
			}
		case <-time.After(5 * time.Second):
			c.Fatalf("Never got anticipated last value from watch: %v.", last)
		}
	}
}

func (s *LeaderSuite) TestStepDown(c *C) {
	const term = 1 * time.Second
	clta := s.newClient(c)
	defer s.closeClient(c, clta)

	cltb := s.newClient(c)
	defer s.closeClient(c, cltb)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string, 2)
	cltb.AddWatchCallback(key, receiver(changeC))

	// add voter a
	clta.AddVoter(context.TODO(), key, "voter a", term)

	// make sure voter a is elected
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter a")
	case <-time.After(2 * term):
		c.Fatal("Timeout waiting for event.")
	}

	// add voter b
	cltb.AddVoter(context.TODO(), key, "voter b", term)

	// tell voter a to step down and wait for the next term
	clta.StepDown(context.TODO())
	time.Sleep(2 * term)

	// make sure voter b is elected
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter b")
	case <-time.After(term):
		c.Fatal("Timeout waiting for event.")
	}
}

func (s *LeaderSuite) TestCanSetupMultipleWatchesOnKey(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())
	changeC := make(chan string)
	clt.AddWatchCallback(key, receiver(changeC))
	clt.AddWatchCallback(key, receiver(changeC))
	clt.AddWatchCallback(key, receiver(changeC))
}

func receiver(ch chan<- string) CallbackFn {
	return func(key, prevVal, newVal string) {
		if newVal == "" || prevVal == newVal {
			return
		}
		ch <- newVal
	}
}

func droppingReceiver(ch chan string) CallbackFn {
	return func(key, prevVal, newVal string) {
		if newVal == "" || prevVal == newVal {
			return
		}
		select {
		case <-ch:
		default:
		}
		ch <- newVal
	}
}
