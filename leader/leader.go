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

package leader

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/gravitational/coordinate/config"

	ebackoff "github.com/cenkalti/backoff"
	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client"
)

var log = logrus.WithField(trace.Component, "leader")

// Config sets leader election configuration options
type Config struct {
	// ETCD defines etcd configuration, client will be instantiated
	// if passed
	ETCD *config.Config
	// Clock is a time provider
	Clock clockwork.Clock
	// Client is ETCD client will be used if passed
	Client client.Client
}

// Client implements ETCD-backed leader election client
// that helps to elect new leaders for a given key and
// monitors the changes to the leaders
type Client struct {
	Config
	client client.Client
	clock  clockwork.Clock
	closeC chan bool
	pauseC chan bool
	closed uint32
}

// NewClient returns a new instance of leader election client
func NewClient(cfg Config) (*Client, error) {
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	var err error
	client := cfg.Client
	if client == nil {
		if cfg.ETCD == nil {
			return nil, trace.BadParameter("expected either ETCD config or Client")
		}
		if err = cfg.ETCD.CheckAndSetDefaults(); err != nil {
			return nil, trace.Wrap(err)
		}
		client, err = cfg.ETCD.NewClient()
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}
	return &Client{
		Config: cfg,
		client: client,
		clock:  cfg.Clock,
		closeC: make(chan bool),
		pauseC: make(chan bool),
	}, nil
}

// CallbackFn specifies callback that is called by AddWatchCallback
// whenever leader changes
type CallbackFn func(key, prevValue, newValue string)

// AddWatchCallback adds the given callback to be invoked when changes are
// made to the specified key's value. The callback is called with new and
// previous values for the key. In the first call, both values are the same
// and reflect the value of the key at that moment
func (l *Client) AddWatchCallback(key string, retry time.Duration, fn CallbackFn) {
	go func() {
		valuesC := make(chan string)
		l.AddWatch(key, retry, valuesC)
		var prev string
		for {
			select {
			case <-l.closeC:
				return
			case val := <-valuesC:
				fn(key, prev, val)
				prev = val
			}
		}
	}()
}

func (l *Client) getWatchAtLatestIndex(ctx context.Context, api client.KeysAPI, key string, retry time.Duration, valuesC chan string) client.Watcher {
	logger := log.WithField("key", key)
	logger.Info("Recreating watch at the latest index.")
	resp := l.getFirstValue(key, retry)
	if resp == nil {
		logger.Info("Client is closing.")
		return nil
	}
	// After reestablishing the watch, always send the value we got to the client.
	if resp.Node != nil {
		logger.WithFields(logrus.Fields{
			"value": resp.Node.Value,
			"index": resp.Index,
		}).Info("Got current value.")
		select {
		case valuesC <- resp.Node.Value:
		case <-l.closeC:
			logger.Info("Watcher has been closed.")
			return nil
		}
	}
	// The watcher that will be receiving events after the value we got above.
	watcher := api.Watcher(key, &client.WatcherOptions{
		// Response.Index corresponds to X-Etcd-Index response header field
		// and is the recommended starting point after a history miss of over
		// 1000 events
		AfterIndex: resp.Index,
	})
	return watcher
}

// AddWatch starts watching the key for changes and sending them
// to the valuesC until the client is stopped.
func (l *Client) AddWatch(key string, retry time.Duration, valuesC chan string) {
	api := client.NewKeysAPI(l.client)

	logger := log.WithField("key", key)
	logger.WithFields(logrus.Fields{
		"peers": l.ETCD.Endpoints,
		"retry": retry,
	}).Info("Setting up watch.")

	go func() {
		var watcher client.Watcher
		var resp *client.Response
		var err error

		ctx, closer := context.WithCancel(context.Background())
		go func() {
			<-l.closeC
			closer()
		}()

		backoff := NewUnlimitedExponentialBackOff()
		ticker := ebackoff.NewTicker(backoff)

		var steps int
		var sentAnything bool

		for {
			// We're either establishing a new watch or it has been reset
			// due to an error/expiration so create a new one at the most
			// recent index.
			if watcher == nil {
				watcher = l.getWatchAtLatestIndex(ctx, api, key, retry, valuesC)
			}

			if watcher != nil {
				resp, err = watcher.Next(ctx)
				if err == nil {
					if resp.Node.Value == "" {
						continue
					}
					backoff.Reset()
				}
			}

			// The watcher has received a new event, send it to the client
			// unless this value has already been sent.
			if err == nil {
				if resp.PrevNode != nil && resp.PrevNode.Value == resp.Node.Value && sentAnything {
					continue
				}
				select {
				case valuesC <- resp.Node.Value:
					sentAnything = true
				case <-l.closeC:
					logger.Info("Watcher is closing.")
					return
				}
				continue
			}

			// Otherwise, the watcher encountered an error while waiting for
			// next event, we may need to recreate it.
			select {
			case <-ticker.C:
				steps += 1
			}

			if IsContextCanceled(err) {
				// The context has been canceled while watcher was waiting
				// for next event.
				logger.Info("Context has been canceled.")
				return
			} else if clusterErr, ok := err.(*client.ClusterError); ok {
				// Got an etcd error, the watcher will retry.
				logger.WithError(clusterErr).Errorf("Etcd error: %v.", clusterErr.Detail())
			} else if IsWatchExpired(err) {
				// The watcher has expired, reset it so it's recreated on the
				// next loop cycle.
				logger.Info("Watch has expired, resetting watch index.")
				watcher = nil
			} else {
				// Unknown error, try recreating the watch after a few repeated
				// unknown errors.
				logger.WithError(err).Error("Unexpected watch error.")
				if steps > 10 {
					watcher = nil
					backoff.Reset()
					steps = 0
				}
			}
		}
	}()
}

// AddVoter starts a goroutine that attempts to set the specified key to
// to the given value with the time-to-live value specified with term.
// The time-to-live value cannot be less than a second.
// After successfully setting the key, it attempts to renew the lease for the specified
// term indefinitely
func (l *Client) AddVoter(context context.Context, key, value string, term time.Duration) error {
	if value == "" {
		return trace.BadParameter("voter value for key cannot be empty")
	}
	if term < time.Second {
		return trace.BadParameter("term cannot be < 1s")
	}
	logger := log.WithFields(logrus.Fields{
		"key":   key,
		"value": value,
		"term":  term,
	})
	go func() {
		err := l.elect(context, key, value, term, logger)
		if err != nil {
			logger.WithError(err).Error("Failed to run election term.")
		}
		ticker := time.NewTicker(term / 5)
		defer ticker.Stop()
		for {
			select {
			case <-l.pauseC:
				logger.Info("Was asked to step down, pausing heartbeat.")
				select {
				case <-time.After(term * 2):
				case <-l.closeC:
					logger.Info("Client closed, removing voter.")
					return
				case <-context.Done():
					logger.Info("Context done, removing voter.")
					return
				}
			default:
			}

			select {
			case <-ticker.C:
				err := l.elect(context, key, value, term, logger)
				if err != nil {
					logger.WithError(err).Error("Failed to run election term.")
				}
			case <-l.closeC:
				logger.Info("Client closed, removing voter.")
				return
			case <-context.Done():
				logger.Info("Context done, removing voter.")
				return
			}
		}
	}()
	return nil
}

// StepDown makes this participant to pause his attempts to re-elect itself thus giving up its leadership
func (l *Client) StepDown() {
	l.pauseC <- true
}

// getFirstValue returns the current value for key if it exists, or waits
// for the value to appear and loops until client.Close is called
func (l *Client) getFirstValue(key string, retryPeriod time.Duration) *client.Response {
	api := client.NewKeysAPI(l.client)
	tick := time.NewTicker(retryPeriod)
	defer tick.Stop()
	for {
		resp, err := api.Get(context.TODO(), key, nil)
		if err == nil {
			return resp
		} else if !IsNotFound(err) {
			log.WithError(err).WithField("key", key).Error("Failed to query key.")
		} else {
			log.WithField("key", key).Info("Key not found, will retry.")
		}
		select {
		case <-tick.C:
		case <-l.closeC:
			log.Info("Watcher got client close signal.")
			return nil
		}
	}
}

// elect is taken from: https://github.com/kubernetes/contrib/blob/master/pod-master/podmaster.go
// this is a slightly modified version though, that does not return the result
// instead we rely on watchers
func (l *Client) elect(ctx context.Context, key, value string, term time.Duration, logger logrus.FieldLogger) error {
	api := client.NewKeysAPI(l.client)
	resp, err := api.Get(ctx, key, nil)
	if err != nil {
		if !IsNotFound(err) {
			return trace.Wrap(err)
		}
		// try to grab the lock for the given term
		_, err := api.Set(ctx, key, value, &client.SetOptions{
			TTL:       term,
			PrevExist: client.PrevNoExist,
		})
		if err != nil {
			return trace.Wrap(err)
		}
		logger.Info("Acquired lease.")
		return nil
	}
	if resp.Node.Value != value {
		return nil
	}
	if resp.Node.Expiration.Sub(l.clock.Now().UTC()) > time.Duration(term/2) {
		return nil
	}

	// extend the lease before the current expries
	_, err = api.Set(ctx, key, value, &client.SetOptions{
		TTL:       term,
		PrevValue: value,
		PrevIndex: resp.Node.ModifiedIndex,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	logger.Debug("Extended lease.")
	return nil
}

// Close stops current operations and releases resources
func (l *Client) Close() error {
	// already closed
	if !atomic.CompareAndSwapUint32(&l.closed, 0, 1) {
		return nil
	}
	close(l.closeC)
	return nil
}

// IsNotFound determines if the specified error identifies a node not found event
func IsNotFound(err error) bool {
	e, ok := err.(client.Error)
	if !ok {
		return false
	}
	return e.Code == client.ErrorCodeKeyNotFound
}

// IsAlreadyExist determines if the specified error identifies a duplicate node event
func IsAlreadyExist(err error) bool {
	e, ok := err.(client.Error)
	if !ok {
		return false
	}
	return e.Code == client.ErrorCodeNodeExist
}

// IsWatchExpired determins if the specified error identifies an expired watch event
func IsWatchExpired(err error) bool {
	switch clientErr := err.(type) {
	case client.Error:
		return clientErr.Code == client.ErrorCodeEventIndexCleared
	}
	return false
}

// IsContextCanceled returns true if the provided error indicates canceled context.
func IsContextCanceled(err error) bool {
	if err == context.Canceled {
		return true
	}
	if clusterErr, ok := err.(*client.ClusterError); ok {
		if len(clusterErr.Errors) != 0 && clusterErr.Errors[0] == context.Canceled {
			return true
		}
	}
	return false
}
