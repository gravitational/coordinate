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

package boltkv

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gravitational/coordinate/defaults"
	"github.com/gravitational/coordinate/kv"
	"github.com/gravitational/coordinate/leader"

	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
)

// Config is a BoltDB configuration
type Config struct {
	// Clock is a clock interface, used in tests
	Clock clockwork.Clock `json:"-"`
	// Codec is used to encode and decode values from the storage
	Codec kv.Codec `json:"-"`
	// Path is a path to DB file
	Path string `json:"path" yaml:"path"`
	// Readonly sets bolt to read only mode
	Readonly bool `json:"readonly" yaml:"readonly"`
	// DBOpenTimeout sets database open timeout
	DBOpenTimeout time.Duration `json:"db_open_timeout" yaml:"db_open_timeout"`
	// FileMask sets file mask
	FileMask int `json:"file_mask" yaml:"file_mask"`
}

func (b *Config) CheckAndSetDefaults() error {
	if b.Codec == nil {
		return trace.BadParameter("missing Codec parameter")
	}
	if b.Path == "" {
		return trace.BadParameter("missing Path parameter")
	}
	path, err := filepath.Abs(b.Path)
	if err != nil {
		return trace.Wrap(err, "expected a valid path")
	}
	b.Path = path
	dir := filepath.Dir(path)
	s, err := os.Stat(dir)
	if err != nil {
		return trace.Wrap(err)
	}
	if !s.IsDir() {
		return trace.BadParameter("path '%v' should be a valid directory", dir)
	}
	if b.Clock == nil {
		b.Clock = clockwork.NewRealClock()
	}
	if b.DBOpenTimeout == 0 {
		b.DBOpenTimeout = defaults.DBOpenTimeout
	}
	if b.FileMask == 0 {
		b.FileMask = defaults.FileMask
	}
	return nil
}

// New returns new BoltDB-backed engine
func New(cfg Config) (*Bolt, error) {
	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	b := &Bolt{
		Config: cfg,
		Clock:  cfg.Clock,
		locks:  make(map[string]time.Time),
	}

	// When opening bolt in read-only mode, make sure bolt properly initializes the database file
	// in case no database file exists before applying the read-only mode
	options := &bolt.Options{Timeout: defaults.DBOpenTimeout, ReadOnly: false}
	_, err := os.Stat(cfg.Path)
	if cfg.Readonly && os.IsNotExist(err) {
		// Have bolt init the data file
		db, err := bolt.Open(cfg.Path, defaults.FileMask, options)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		db.Close()
	}

	options.ReadOnly = cfg.Readonly
	db, err := bolt.Open(cfg.Path, defaults.FileMask, options)
	if err != nil {
		if err == bolt.ErrTimeout {
			return nil, trace.ConnectionProblem(nil, "local storage at %v is locked. Another instance is running?", cfg.Path)
		}
		return nil, trace.Wrap(err)
	}
	b.db = db
	if !cfg.Readonly {
		log.Infof("BOLT: locked %v", b.Path)
	}
	return b, nil
}

// Bolt is a BoltDB-backend engine
type Bolt struct {
	sync.Mutex
	Config
	clockwork.Clock
	db    *bolt.DB
	locks map[string]time.Time
}

func (b *Bolt) Key(prefix string, keys ...string) kv.Key {
	return append([]string{"root", prefix}, keys...)
}

func (b *Bolt) split(key kv.Key) ([]string, string) {
	return key[:len(key)-1], key[len(key)-1]
}

func (b *Bolt) GetLeader() leader.Leader {
	panic("this backend does not implement leader election")
}

func upsertBucket(tx *bolt.Tx, buckets []string) (*bolt.Bucket, error) {
	bkt, err := tx.CreateBucketIfNotExists([]byte(buckets[0]))
	if err != nil {
		return nil, trace.Wrap(boltErr(err))
	}
	for _, key := range buckets[1:] {
		bkt, err = bkt.CreateBucketIfNotExists([]byte(key))
		if err != nil {
			return nil, trace.Wrap(boltErr(err))
		}
	}
	return bkt, nil
}

func createBucket(tx *bolt.Tx, buckets []string) (*bolt.Bucket, error) {
	bkt, err := tx.CreateBucketIfNotExists([]byte(buckets[0]))
	if err != nil {
		return nil, trace.Wrap(boltErr(err))
	}
	rest := buckets[1:]
	for i, key := range rest {
		if i == len(rest)-1 {
			bkt, err = bkt.CreateBucket([]byte(key))
			if err != nil {
				return nil, trace.Wrap(boltErr(err))
			}
		} else {
			bkt, err = bkt.CreateBucketIfNotExists([]byte(key))
			if err != nil {
				return nil, trace.Wrap(boltErr(err))
			}
		}
	}
	return bkt, nil
}

func getBucket(tx *bolt.Tx, buckets []string) (*bolt.Bucket, error) {
	bkt := tx.Bucket([]byte(buckets[0]))
	if bkt == nil {
		return nil, trace.NotFound("bucket %v not found", buckets[0])
	}
	for _, key := range buckets[1:] {
		bkt = bkt.Bucket([]byte(key))
		if bkt == nil {
			return nil, trace.NotFound("bucket %v not found", key)
		}
	}
	return bkt, nil
}

func (b *Bolt) CreateDir(key kv.Key, ttl time.Duration) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		_, err := createBucket(tx, key)
		return trace.Wrap(boltErr(err))
	})
}

func (b *Bolt) UpsertDir(key kv.Key, ttl time.Duration) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		_, err := upsertBucket(tx, key)
		return trace.Wrap(boltErr(err))
	})
}

func (b *Bolt) CreateVal(k kv.Key, val interface{}, ttl time.Duration) error {
	encoded, err := b.Codec.EncodeToBytes(val)
	if err != nil {
		return trace.Wrap(err)
	}
	buckets, key := b.split(k)
	log.Infof("createVal: %v %v", buckets, key)
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := upsertBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		val := bkt.Get([]byte(key))
		if val != nil {
			return trace.AlreadyExists("'%v' already exists", key)
		}
		return bkt.Put([]byte(key), encoded)
	})
}

func (b *Bolt) UpsertVal(k kv.Key, val interface{}, ttl time.Duration) error {
	encoded, err := b.Codec.EncodeToBytes(val)
	if err != nil {
		return trace.Wrap(err)
	}
	buckets, key := b.split(k)
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := upsertBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		return bkt.Put([]byte(key), encoded)
	})
}

func (b *Bolt) UpdateVal(k kv.Key, val interface{}, ttl time.Duration) error {
	encoded, err := b.Codec.EncodeToBytes(val)
	if err != nil {
		return trace.Wrap(err)
	}
	buckets, key := b.split(k)
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := upsertBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		val := bkt.Get([]byte(key))
		if val == nil {
			return trace.NotFound("'%v' not found", key)
		}
		return bkt.Put([]byte(key), encoded)
	})
}

func (b *Bolt) CompareAndSwap(k kv.Key, val interface{}, prevVal interface{}, outVal interface{}, ttl time.Duration) error {
	encoded, err := b.Codec.EncodeToBytes(val)
	if err != nil {
		return trace.Wrap(err)
	}
	var prevEncoded []byte
	if prevVal != nil {
		prevEncoded, err = b.Codec.EncodeToBytes(prevVal)
		if err != nil {
			return trace.Wrap(err)
		}
	}

	buckets, key := b.split(k)
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := upsertBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		currentEncoded := bkt.Get([]byte(key))
		if prevVal == nil { // we don't expect the value to exist
			if currentEncoded != nil {
				return trace.AlreadyExists("key '%v' already exists", key)
			}
			return trace.Wrap(bkt.Put([]byte(key), encoded))
		} else { // we expect the previous value to exist
			if val == nil {
				return trace.NotFound("key '%v' not found", key)
			}
			if bytes.Compare(currentEncoded, prevEncoded) != 0 {
				return trace.CompareFailed("expected %v got %v", string(prevEncoded), string(currentEncoded))
			}
			err = bkt.Put([]byte(key), encoded)
			if err != nil {
				return trace.Wrap(err)
			}
			return trace.Wrap(b.Codec.DecodeFromBytes(currentEncoded, outVal))
		}
	})
}

func (b *Bolt) GetVal(k kv.Key, outVal interface{}) error {
	buckets, key := b.split(k)
	return b.db.View(func(tx *bolt.Tx) error {
		bkt, err := getBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		bytes := bkt.Get([]byte(key))
		if bytes == nil {
			_, err := getBucket(tx, append(buckets, key))
			if err == nil {
				return trace.BadParameter("key '%v 'is a bucket", key)
			}
			return trace.NotFound("%v %v not found", buckets, key)
		}
		return b.Codec.DecodeFromBytes(bytes, outVal)
	})
}

func (b *Bolt) DeleteKey(k kv.Key) error {
	buckets, key := b.split(k)
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := getBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		if bkt.Get([]byte(key)) == nil {
			return trace.NotFound("%v is not found", key)
		}
		return bkt.Delete([]byte(key))
	})
}

func (b *Bolt) DeleteKeyIf(k kv.Key, expected interface{}) error {
	if expected == nil {
		return trace.BadParameter("missing parameter 'expected'")
	}
	encoded, err := b.Codec.EncodeToBytes(expected)
	if err != nil {
		return trace.Wrap(err)
	}
	buckets, key := b.split(k)
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := getBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		existing := bkt.Get([]byte(key))
		if existing == nil {
			return trace.NotFound("%v is not found", key)
		}
		if bytes.Compare(existing, encoded) != 0 {
			return trace.CompareFailed("expected %v got %v", string(encoded), string(existing))
		}
		return bkt.Delete([]byte(key))
	})
}

func (b *Bolt) DeleteDir(k kv.Key) error {
	buckets, key := b.split(k)
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := getBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		err = bkt.DeleteBucket([]byte(key))
		if err != nil {
			return trace.NotFound("%v is not found", key)
		}
		return nil
	})
}

func (b *Bolt) AcquireLock(token kv.Key, ttl time.Duration) error {
	for {
		err := b.TryAcquireLock(token, ttl)
		if err != nil {
			if !trace.IsCompareFailed(err) && !trace.IsAlreadyExists(err) {
				return trace.Wrap(err)
			}
			time.Sleep(defaults.DelayBetweenLockAttempts)
		} else {
			return nil
		}
	}
}

func (b *Bolt) TryAcquireLock(key kv.Key, ttl time.Duration) error {
	return b.CreateVal(key, "locked", ttl)
}

func (b *Bolt) ReleaseLock(key kv.Key) error {
	return b.DeleteKey(key)
}

func (b *Bolt) GetKeys(key kv.Key) ([]string, error) {
	out := []string{}
	buckets := key
	err := b.db.View(func(tx *bolt.Tx) error {
		bkt, err := getBucket(tx, buckets)
		if err != nil {
			if trace.IsNotFound(err) {
				return nil
			}
			return trace.Wrap(err)
		}
		c := bkt.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			out = append(out, string(k))
		}
		return nil
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	sort.Strings(out)
	return out, nil
}

func (b *Bolt) IsDir(key kv.Key) (bool, error) {
	var out interface{}
	err := b.GetVal(key, &out)
	if err == nil {
		return false, nil
	}
	err = b.db.View(func(tx *bolt.Tx) error {
		_, err := getBucket(tx, key)
		return err
	})
	if err != nil {
		return false, trace.Wrap(err)
	}
	return true, nil
}

// Close closes the backend resources
func (b *Bolt) Close() error {
	log.Infof("BOLT closing: %v", b.Path)
	return b.db.Close()
}

func boltErr(err error) error {
	if err == bolt.ErrBucketNotFound {
		return trace.NotFound(err.Error())
	}
	if err == bolt.ErrBucketExists {
		return trace.AlreadyExists(err.Error())
	}
	return err
}
