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
	"sort"
	"strings"
	"time"

	"github.com/gravitational/coordinate/config"
	"github.com/gravitational/coordinate/kv"
	"github.com/gravitational/coordinate/leader"

	"github.com/coreos/etcd/client"
	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	"golang.org/x/net/context"
)

const (
	// DelayBetweenLockAttempts is a delay between attempt interval for AcuireLock
	DelayBetweenLockAttempts = 500 * time.Millisecond
)

// Config represents JSON config for ETCD backend
type Config struct {
	config.Config
	Clock clockwork.Clock `json:"-"`
	Codec kv.Codec        `json:"-"`
}

// Check checks if all the parameters are valid and sets defaults
func (cfg *Config) CheckAndSetDefaults() error {
	if err := cfg.Config.CheckAndSetDefaults(); err != nil {
		return trace.Wrap(err)
	}
	if cfg.Codec == nil {
		return trace.BadParameter("missing parameter Codec")
	}
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	return nil
}

// New returns new etcd client engine with some useful wrappers
func New(cfg Config) (*ETCD, error) {
	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	e := &ETCD{
		Config:  cfg,
		Clock:   cfg.Clock,
		etcdKey: strings.Split(cfg.Key, "/"),
		cancelC: make(chan bool, 1),
		stopC:   make(chan bool, 1),
	}
	if err := e.reconnect(); err != nil {
		return nil, trace.Wrap(err)
	}
	leader, err := NewLeader(LeaderConfig{Client: e.client, Clock: cfg.Clock})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	e.leader = leader
	return e, nil
}

// ETCD is implementation of etcd key value engine
type ETCD struct {
	Config
	clockwork.Clock
	etcdKey []string
	client  client.Client
	api     client.KeysAPI
	cancelC chan bool
	stopC   chan bool
	leader  leader.Leader
}

func (e *ETCD) GetLeader() leader.Leader {
	return e.leader
}

func (e *ETCD) Key(prefix string, keys ...string) kv.Key {
	key := make([]string, 0, len(e.etcdKey)+len(keys)+1)
	key = append(key, e.etcdKey...)
	key = append(key, prefix)
	key = append(key, keys...)
	for i := range key {
		key[i] = strings.Replace(key[i], "/", "%2F", -1)
	}
	return key
}

// ekey returns etcd formatted key
func ekey(key kv.Key) string {
	out := strings.Join(key, "/")
	return out
}

func (e *ETCD) Close() error {
	return nil
}

func (e *ETCD) reconnect() error {
	clt, err := e.NewClient()
	if err != nil {
		return trace.Wrap(err)
	}
	e.client = clt
	e.api = client.NewKeysAPI(e.client)

	return nil
}

func (e *ETCD) CreateVal(key kv.Key, val interface{}, ttl time.Duration) error {
	encoded, err := e.Codec.EncodeToString(val)
	if err != nil {
		return trace.Wrap(err)
	}
	_, err = e.api.Set(
		context.TODO(), ekey(key), encoded,
		&client.SetOptions{PrevExist: client.PrevNoExist, TTL: ttl})
	return trace.Wrap(ConvertErr(err))
}

func (e *ETCD) CreateDir(key kv.Key, ttl time.Duration) error {
	_, err := e.api.Set(
		context.TODO(), ekey(key), "",
		&client.SetOptions{PrevExist: client.PrevNoExist, TTL: ttl, Dir: true})
	return trace.Wrap(ConvertErr(err))
}

func (e *ETCD) UpsertDir(key kv.Key, ttl time.Duration) error {
	err := ConvertErr(e.CreateDir(key, ttl))
	if err != nil {
		if !trace.IsAlreadyExists(err) {
			return trace.Wrap(err)
		}
	}
	_, err = e.api.Set(
		context.TODO(), ekey(key), "",
		&client.SetOptions{TTL: ttl, Dir: true, PrevExist: client.PrevExist})
	return trace.Wrap(ConvertErr(err))
}

func (e *ETCD) UpsertVal(key kv.Key, val interface{}, ttl time.Duration) error {
	encoded, err := e.Codec.EncodeToString(val)
	if err != nil {
		return trace.Wrap(err)
	}
	_, err = e.api.Set(
		context.TODO(), ekey(key), encoded, &client.SetOptions{TTL: ttl})
	return ConvertErr(err)
}

func (e *ETCD) UpdateVal(key kv.Key, val interface{}, ttl time.Duration) error {
	encoded, err := e.Codec.EncodeToString(val)
	if err != nil {
		return trace.Wrap(err)
	}
	_, err = e.api.Set(
		context.TODO(), ekey(key), encoded, &client.SetOptions{TTL: ttl, PrevExist: client.PrevExist})
	return ConvertErr(err)
}

func (e *ETCD) CompareAndSwap(key kv.Key, val interface{}, prevVal interface{}, outVal interface{}, ttl time.Duration) error {
	encoded, err := e.Codec.EncodeToString(val)
	if err != nil {
		return trace.Wrap(err)
	}
	var re *client.Response
	if prevVal != nil {
		encodedPrev, err := e.Codec.EncodeToString(prevVal)
		if err != nil {
			return trace.Wrap(err)
		}
		re, err = e.api.Set(
			context.TODO(), ekey(key), encoded,
			&client.SetOptions{TTL: ttl, PrevValue: encodedPrev, PrevExist: client.PrevExist})
	} else {
		re, err = e.api.Set(
			context.TODO(),
			ekey(key), encoded,
			&client.SetOptions{TTL: ttl, PrevExist: client.PrevNoExist})
	}
	err = ConvertErr(err)
	if err != nil {
		return trace.Wrap(err)
	}
	if re.PrevNode != nil {
		err = e.Codec.DecodeFromString(re.PrevNode.Value, outVal)
		return trace.Wrap(err)
	}
	return nil
}

func (e *ETCD) GetVal(key kv.Key, val interface{}) error {
	re, err := e.api.Get(context.TODO(), ekey(key), nil)
	if err != nil {
		return ConvertErr(err)
	}
	if re.Node.Dir {
		return trace.BadParameter("'%v': trying to get value of bucket", key)
	}
	err = e.Codec.DecodeFromString(re.Node.Value, val)
	return trace.Wrap(err)
}

// DeleteKeyIf deletes key if the previous version matches the passed one
func (e *ETCD) DeleteKeyIf(key kv.Key, expected interface{}) error {
	if expected == nil {
		return trace.BadParameter("missing parameter 'expected'")
	}
	encoded, err := e.Codec.EncodeToString(expected)
	if err != nil {
		return trace.Wrap(err)
	}
	_, err = e.api.Delete(context.TODO(), ekey(key),
		&client.DeleteOptions{
			Dir:       false,
			PrevValue: encoded,
		})
	return ConvertErr(err)
}

func (e *ETCD) DeleteKey(key kv.Key) error {
	_, err := e.api.Delete(context.TODO(), ekey(key), nil)
	return ConvertErr(err)
}

func (e *ETCD) DeleteDir(key kv.Key) error {
	_, err := e.api.Delete(context.TODO(), ekey(key),
		&client.DeleteOptions{Dir: true, Recursive: true})
	return ConvertErr(err)
}

func (e *ETCD) AcquireLock(token kv.Key, ttl time.Duration) error {
	for {
		err := e.TryAcquireLock(token, ttl)
		if err != nil {
			if !trace.IsCompareFailed(err) && !trace.IsAlreadyExists(err) {
				return trace.Wrap(err)
			}
			time.Sleep(DelayBetweenLockAttempts)
		} else {
			return nil
		}
	}
}

func (e *ETCD) TryAcquireLock(key kv.Key, ttl time.Duration) error {
	_, err := e.api.Set(
		context.TODO(),
		ekey(key), "locked",
		&client.SetOptions{TTL: ttl, PrevExist: client.PrevNoExist})
	return ConvertErr(err)
}

func (e *ETCD) ReleaseLock(key kv.Key) error {
	_, err := e.api.Delete(context.TODO(), ekey(key), nil)
	return ConvertErr(err)
}

func (e *ETCD) IsDir(key kv.Key) (bool, error) {
	re, err := e.api.Get(context.TODO(), ekey(key), nil)
	if err != nil {
		return false, ConvertErr(err)
	}
	return isDir(re.Node), nil
}

func (e *ETCD) GetKeys(key kv.Key) ([]string, error) {
	var vals []string
	re, err := e.api.Get(context.TODO(), ekey(key), nil)
	err = ConvertErr(err)
	if err != nil {
		if trace.IsNotFound(err) {
			return vals, nil
		}
		return nil, trace.Wrap(err)
	}
	if !isDir(re.Node) {
		return nil, trace.BadParameter("'%v': expected directory", key)
	}
	for _, n := range re.Node.Nodes {
		vals = append(vals, suffix(n.Key))
	}
	sort.Sort(sort.StringSlice(vals))
	return vals, nil
}

// ConvertErr converts error from etcd error to trace error
func ConvertErr(e error) error {
	if e == nil {
		return nil
	}
	switch err := e.(type) {
	case *client.ClusterError:
		return trace.Wrap(err, err.Detail())
	case client.Error:
		switch err.Code {
		case client.ErrorCodeKeyNotFound:
			return trace.NotFound(err.Error())
		case client.ErrorCodeNotFile:
			return trace.BadParameter(err.Error())
		case client.ErrorCodeNodeExist:
			return trace.AlreadyExists(err.Error())
		case client.ErrorCodeTestFailed:
			return trace.CompareFailed(err.Error())
		}
	}
	return e
}

func isDir(n *client.Node) bool {
	return n != nil && n.Dir == true
}

func suffix(key string) string {
	vals := strings.Split(key, "/")
	return vals[len(vals)-1]
}
