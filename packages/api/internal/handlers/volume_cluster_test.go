package handlers

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// A team row holding the local cluster's zero UUID is on the default cluster.
// Treating it as BYOC sent the orchestrator the placeholder volume type, which
// it refused as an unknown volume type.
func TestUsesCustomCluster(t *testing.T) {
	t.Parallel()

	teamOn := func(clusterID *uuid.UUID) *types.Team {
		return &types.Team{Team: &authqueries.Team{ClusterID: clusterID}}
	}
	local := consts.LocalClusterID
	custom := uuid.New()

	assert.False(t, usesCustomCluster(nil), "no team")
	assert.False(t, usesCustomCluster(teamOn(nil)), "NULL cluster_id")
	assert.False(t, usesCustomCluster(teamOn(&local)), "zero UUID is the local cluster")
	assert.True(t, usesCustomCluster(teamOn(&custom)), "a real BYOC cluster")
}
