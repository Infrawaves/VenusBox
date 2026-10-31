package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNodeSyncInterval(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 5*time.Second, nodeSyncInterval(5*time.Second))
	assert.Equal(t, cacheSyncTime, nodeSyncInterval(0))
	assert.Equal(t, cacheSyncTime, nodeSyncInterval(-time.Second))
}
