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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/gravitational/coordinate/kv"

	"github.com/coreos/etcd/client"
	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	"golang.org/x/net/context"
)

// TempEngine helps to create and destroy ad-hock
// databases in Etcd
type TempEngine struct {
	API    client.KeysAPI
	Prefix string
	Clock  clockwork.FakeClock
	Engine *ETCD
}

func (t *TempEngine) Delete() error {
	var err error
	if t.API != nil {
		_, err = t.API.Delete(context.Background(), t.Prefix, &client.DeleteOptions{Recursive: true, Dir: true})
		err = ConvertErr(err)
		if err != nil && !trace.IsNotFound(err) {
			return trace.Wrap(err)
		}
		return nil
	}
	return nil
}

func NewTemp(configJSON string) (*TempEngine, error) {
	if configJSON == "" {
		return nil, trace.BadParameter("missing ETCD configuration")
	}
	fakeClock := clockwork.NewFakeClock()
	cfg := Config{Clock: fakeClock}
	err := json.Unmarshal([]byte(configJSON), &cfg)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	token, err := CryptoRandomHex(6)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	cfg.Key = fmt.Sprintf("%v/%v", cfg.Key, token)
	cfg.Codec = &kv.JSONCodec{}

	engine, err := New(cfg)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	api := client.NewKeysAPI(engine.client)

	return &TempEngine{Prefix: cfg.Key, API: api, Clock: fakeClock, Engine: engine}, nil
}

// CryptoRandomHex returns hex encoded random string generated with crypto-strong
// pseudo random generator of the given bytes
func CryptoRandomHex(len int) (string, error) {
	randomBytes := make([]byte, len)
	if _, err := rand.Reader.Read(randomBytes); err != nil {
		return "", trace.Wrap(err)
	}
	return hex.EncodeToString(randomBytes), nil
}
