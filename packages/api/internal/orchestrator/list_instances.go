package orchestrator

import (
	"context"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
)

// GetSandboxes returns instances for a given team.
func (o *Orchestrator) GetSandboxes(ctx context.Context, teamID uuid.UUID, states []sandbox.State) ([]sandbox.Sandbox, error) {
	ctx, childSpan := tracer.Start(ctx, "get-sandboxes")
	defer childSpan.End()

	return o.sandboxStore.TeamItems(ctx, teamID, states)
}

// GetAllSandboxes returns instances across all teams, for platform-wide admin views.
func (o *Orchestrator) GetAllSandboxes(ctx context.Context, states []sandbox.State) ([]sandbox.Sandbox, error) {
	ctx, childSpan := tracer.Start(ctx, "get-all-sandboxes")
	defer childSpan.End()

	return o.sandboxStore.AllItems(ctx, states)
}
