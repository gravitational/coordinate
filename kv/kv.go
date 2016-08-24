package kv

import (
	"io"
	"time"

	"github.com/gravitational/coordinate/leader"
	"github.com/jonboulle/clockwork"
)

// Forever indicates that the value does not expire
const Forever = 0

// Key is a multi-part key where every part is a separate string in a slice
type Key []string

// Engine is interface to the Key-Value storage
type Engine interface {
	// Closer closes resources associated with the engine
	io.Closer
	// Key creats new key
	Key(prefix string, keys ...string) Key
	// CreateVal creates new value
	CreateVal(key Key, val interface{}, ttl time.Duration) error
	// UpsertVal updates or inserts value
	UpsertVal(key Key, val interface{}, ttl time.Duration) error
	// UpdateVal updates value
	UpdateVal(key Key, val interface{}, ttl time.Duration) error
	// CompareAndSwap implements Compare and swap operation for key
	CompareAndSwap(key Key, val interface{}, prevVal interface{}, newVal interface{}, ttl time.Duration) error
	// GetVal returns and sets value val
	GetVal(key Key, val interface{}) error
	// DeleteKey deletes key
	DeleteKey(key Key) error
	// CreateDir creates new directory
	CreateDir(key Key, ttl time.Duration) error
	// UpsertDir creates or updates existing directory
	UpsertDir(key Key, ttl time.Duration) error
	// DeleteDir deletes the lock
	DeleteDir(key Key) error
	// AcquireLock blocks until it can acquire lock
	AcquireLock(token Key, ttl time.Duration) error
	// TryAcquireLock tries to acquire lock and fails if it's taken
	TryAcquireLock(token Key, ttl time.Duration) error
	// ReleaseLock releases the named lock
	ReleaseLock(token Key) error
	// GetKeys returns a list of sub-keys for given key
	GetKeys(key Key) ([]string, error)
	// GetLeader returns leader election client
	GetLeader() leader.Leader
}

// UTC sets the stored timezone to UTC in time struct
func UTC(t *time.Time) {
	if t.IsZero() {
		// to fix issue with timezones for tests
		*t = time.Time{}
		return
	}
	*t = t.UTC()
}

// TTL converts time set in the future into TTL
func TTL(clock clockwork.Clock, t time.Time) time.Duration {
	if t.IsZero() {
		return Forever
	}
	diff := t.UTC().Sub(clock.Now().UTC())
	if diff < 0 {
		return Forever
	}
	return diff
}
