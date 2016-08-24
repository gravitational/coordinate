package defaults

import (
	"time"
)

const (
	// TestETCD is a name for test ETCD environment variable turning tests on/off
	TestETCD = "TEST_ETCD"
	// TestETCDConfig is a name of environment variaable with JSON blob of ETCD config
	TestETCDConfig = "TEST_ETCD_CONFIG"
	// DBOpenTimeout is a default timeout for opening the DB
	DBOpenTimeout = 30 * time.Second
	// FileMask is a mask set on new files by default
	FileMask = 0600
	// DelayBetweenLockAttempts is a default delay for AcquireLock
	DelayBetweenLockAttempts = 500 * time.Millisecond
)
