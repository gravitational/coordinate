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

package kv

import (
	"encoding/base64"
	"encoding/json"

	"github.com/gravitational/trace"
)

// Codec is responsible for encoding/decoding objects
type Codec interface {
	EncodeToString(val interface{}) (string, error)
	EncodeToBytes(val interface{}) ([]byte, error)
	DecodeFromString(val string, in interface{}) error
	DecodeFromBytes(val []byte, in interface{}) error
}

// JSONCodec is codec designed for etcd 2.x series that don't
// reliably support binary data, so it adds additional base64 encoding
// to JSON-serialized values. We can drop this support once we move to 3.x
// series
type JSONCodec struct {
}

func (*JSONCodec) EncodeToString(val interface{}) (string, error) {
	data, err := json.Marshal(val)
	if err != nil {
		return "", trace.Wrap(err, "failed to encode object")
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func (*JSONCodec) EncodeToBytes(val interface{}) ([]byte, error) {
	data, err := json.Marshal(val)
	if err != nil {
		return nil, trace.Wrap(err, "failed to encode object")
	}
	return data, nil
}

func (*JSONCodec) DecodeFromString(val string, in interface{}) error {
	data, err := base64.StdEncoding.DecodeString(val)
	if err != nil {
		return trace.Wrap(err, "failed to decode object")
	}
	err = json.Unmarshal([]byte(data), &in)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (*JSONCodec) DecodeFromBytes(data []byte, in interface{}) error {
	err := json.Unmarshal(data, &in)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}
