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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gravitational/coordinate/config"
	"github.com/gravitational/trace"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
	"github.com/coreos/etcd/client"
	"github.com/jonboulle/clockwork"
)

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
	client client.Client
	clock  clockwork.Clock
	closeC chan struct{}
	pauseC chan struct{}
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
		client: client,
		clock:  cfg.Clock,
		closeC: make(chan struct{}),
		pauseC: make(chan struct{}),
	}, nil
}

// CallbackFn specifies callback that is called by AddWatchCallback
// whenever leader changes
type CallbackFn func(key, prevValue, newValue string)

// AddWatchCallback adds the given callback to be invoked when changes are
// made to the specified key's value. The callback is called with new and
// previous values for the key. In the first call, both values are the same
// and reflect the value of the key at that moment
func (l *Client) AddWatchCallback(ctx context.Context, key string, retry time.Duration, fn CallbackFn) {
	go func() {
		valuesC := make(chan string)
		l.AddWatch(ctx, key, retry, valuesC)
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

func (l *Client) recreateWatchAtLatestIndex(ctx context.Context, api client.KeysAPI, key string, entry *log.Entry) (client.Watcher, *client.Response, error) {
	resp, err := api.Get(ctx, key, nil)
	if err != nil {
		return nil, nil, trace.Wrap(err, "failed to fetch value")
	}
	if resp == nil {
		return nil, nil, nil
	}

	watcher := api.Watcher(key, &client.WatcherOptions{
		// Response.Index corresponds to X-Etcd-Index response header field
		// and is the recommended starting point after a history miss of over
		// 1000 events
		AfterIndex: resp.Index,
	})
	return watcher, resp, nil
}

// AddWatch starts watching the key for changes and sending them
// to the valuesC.
// The watch can be stopped either with the specified context or Client.Close
func (l *Client) AddWatch(ctx context.Context, key string, retry time.Duration, valuesC chan string) {
	api := client.NewKeysAPI(l.client)
	entry := log.WithFields(log.Fields{"prefix": fmt.Sprintf("AddWatch(key=%v)", key)})
	renewCtx, cancel := context.WithCancel(ctx)
	renew := func() (client.Watcher, *client.Response) {
		b := NewUnlimitedExponentialBackOff()
		b.InitialInterval = retry
		var watcher client.Watcher
		var resp *client.Response
		backoff.Retry(func() (err error) {
			watcher, resp, err = l.recreateWatchAtLatestIndex(renewCtx, api, key, entry)
			return trace.Wrap(err)
		}, b)
		return watcher, resp
	}

	go l.watcher(ctx, renew, cancel, valuesC, entry)
}

func (l *Client) watcher(ctx context.Context, renew renewFunc, cancel context.CancelFunc, valuesC chan string, entry *log.Entry) {
	defer cancel()

	watcher, resp := renew()
	if watcher == nil {
		return
	}

	// send sends the response value to the valuesC channel.
	// Returns false if the close notification has been received
	send := func() bool {
		if resp.Node.Value == "" {
			return true
		}

		// do not resend the same value twice
		if resp.PrevNode != nil && resp.PrevNode.Value == resp.Node.Value {
			return true
		}

		select {
		case valuesC <- resp.Node.Value:
		case <-ctx.Done():
			return false
		case <-l.closeC:
			return false
		}
		return true
	}

	backOff := NewUnlimitedExponentialBackOff()
	ticker := backoff.NewTicker(backOff)
	var steps int

	var err error
	for {
		if !send() {
			return
		}

		resp, err = watcher.Next(ctx)
		if err == nil {
			backOff.Reset()
			continue
		}

		select {
		case <-ticker.C:
			steps += 1
		case <-ctx.Done():
			return
		case <-l.closeC:
			return
		}

		if err == context.Canceled {
			return
		} else if cerr, ok := err.(*client.ClusterError); ok {
			if len(cerr.Errors) != 0 && cerr.Errors[0] == context.Canceled {
				return
			}
			entry.Warningf("unexpected cluster error: %v (%v)", err, cerr.Detail())
			continue
		} else if IsWatchExpired(err) {
			entry.Debug("watch expired, resetting watch index")
			watcher, resp = renew()
			if watcher == nil {
				return
			}
		} else {
			entry.Warningf("unexpected watch error: %v", trace.DebugReport(err))
			// try recreating the watch if we get repeated unknown errors
			if steps > 10 {
				watcher, resp = renew()
				if watcher == nil {
					return
				}
				backOff.Reset()
				steps = 0
			}
			continue
		}
	}
}

type renewFunc func() (client.Watcher, *client.Response)

// AddVoter starts a goroutine that attempts to set the specified key to
// to the given value with the time-to-live value specified with term.
// The term cannot be less than a second.
// After successfully setting the key, it attempts to renew the lease for the specified
// term indefinitely
func (l *Client) AddVoter(ctx context.Context, key, value string, term time.Duration) error {
	if value == "" {
		return trace.BadParameter("voter value for key cannot be empty")
	}
	if term < time.Second {
		return trace.BadParameter("term cannot be < 1s")
	}

	go l.voter(ctx, key, value, term)
	return nil
}

func (l *Client) voter(ctx context.Context, key, value string, term time.Duration) {
	log := log.WithFields(log.Fields{"value": value})

	b := NewFlippingBackOff(
		backoff.NewConstantBackOff(term/5),
		NewUnlimitedExponentialBackOff(),
	)

	ticker := backoff.NewTicker(b)
	defer ticker.Stop()
	for {
		select {
		case <-l.pauseC:
			log.Debug("was asked to step down, pausing heartbeat")
			select {
			case <-time.After(term * 2):
			case <-l.closeC:
				return
			case <-ctx.Done():
				return
			}
		case <-ticker.C:
			err := l.elect(ctx, key, value, term)
			if err != nil {
				log.Warningf("voter error: %v", trace.DebugReport(err))
				b.SetFailing(true)
			} else {
				b.SetFailing(false)
			}
		case <-l.closeC:
			return
		case <-ctx.Done():
			return
		}
	}
}

// StepDown makes this participant to pause his attempts to re-elect itself thus giving up its leadership
func (l *Client) StepDown() {
	l.pauseC <- struct{}{}
}

// elect is taken from: https://github.com/kubernetes/contrib/blob/master/pod-master/podmaster.go
// this is a slightly modified version though, that does not return the result
// instead we rely on watchers
func (l *Client) elect(ctx context.Context, key, value string, term time.Duration) error {
	candidate := fmt.Sprintf("candidate(key=%v, value=%v, term=%v)", key, value, term)
	log := log.WithFields(log.Fields{"candidate": candidate})

	api := client.NewKeysAPI(l.client)
	resp, err := api.Get(ctx, key, nil)
	if err != nil {
		if !IsNotFound(err) {
			return trace.Wrap(err)
		}
		log.Debug("key not found, try to elect myself")
		// try to grab the lock for the given term
		_, err := api.Set(ctx, key, value, &client.SetOptions{
			TTL:       term,
			PrevExist: client.PrevNoExist,
		})
		if err != nil {
			return trace.Wrap(err)
		}
		log.Debug("successfully elected")
		return nil
	}
	if resp.Node.Value != value {
		log.Debugf("leader is %q, try again", resp.Node.Value)
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

	log.Debug("extended lease")
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
