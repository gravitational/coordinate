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
	"io"
	"time"
)

// CallbackFn specifies callback that is called by AddWatchCallback
// whenever leader changes
type CallbackFn func(key, prevValue, newValue string)

// Leader specifies interface managing leader election process
type Leader interface {
	// Closer closes leader election activites and frees resources
	io.Closer

	// AddWatchCallback adds the given callback to be invoked when changes are
	// made to the specified key's value. The callback is called with new and
	// previous values for the key. In the first call, both values are the same
	// and reflect the value of the key at that moment
	AddWatchCallback(key string, retry time.Duration, fn CallbackFn)

	// AddWatch starts watching the key for changes and sending them
	// to the valuesC, the watch is stopped
	AddWatch(key string, retry time.Duration, valuesC chan string)

	// AddVoter adds a voter that tries to elect given value
	// by attempting to set the key to the value for a given term duration
	// it also attempts to hold the lease indefinitely
	AddVoter(key, value string, term time.Duration) error
}
