package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
)

func TestKillFaultDomain(t *testing.T) {
	t.Parallel()

	for reason, want := range map[string]faultDomain{
		"request":               faultDomainUser,
		"timeout":               faultDomainUser,
		"team_deleted":          faultDomainUser,
		"admin":                 faultDomainPlatform,
		"orphaned":              faultDomainPlatform,
		"base_template_missing": faultDomainPlatform,
		killReasonSealFailed:    faultDomainPlatform,
		killReasonResumeFailed:  faultDomainPlatform,
		killReasonThawFailed:    faultDomainPlatform,
		killReasonUploadFailed:  faultDomainPlatform,
		killReasonUnknown:       faultDomainUnknown,
		"":                      faultDomainUnknown,
		"something-new":         faultDomainUnknown,
	} {
		assert.Equal(t, want, killFaultDomain(reason), reason)
	}
}

func TestCrashFaultDomain(t *testing.T) {
	t.Parallel()

	assert.Equal(t, faultDomainUser, crashFaultDomain(fc.CrashCauseCleanExit, 1))
	assert.Equal(t, faultDomainUnknown, crashFaultDomain(fc.CrashCauseCleanExit, 0))
	assert.Equal(t, faultDomainUnknown, crashFaultDomain(fc.CrashCauseUnknown, 3))
	for _, cause := range []fc.CrashCause{
		fc.CrashCauseExitError, fc.CrashCauseExternalSignal, fc.CrashCauseFault,
		fc.CrashCauseMemoryHandlerFailed, fc.CrashCauseRequestedSignal,
	} {
		assert.Equal(t, faultDomainPlatform, crashFaultDomain(cause, 0), string(cause))
	}
}

func TestClassifyExecutionEnd(t *testing.T) {
	t.Parallel()

	newSbx := func() *sandbox.Sandbox { return &sandbox.Sandbox{Metadata: &sandbox.Metadata{}} }

	// No stop reason reads as a crash; with no process the cause is unknown.
	end := classifyExecutionEnd(newSbx(), nil)
	assert.Equal(t, sandbox.StopReasonCrashed, end.stopReason)
	assert.Equal(t, fc.CrashCauseUnknown, end.crashCause)
	assert.Equal(t, faultDomainUnknown, end.faultDomain)

	paused := newSbx()
	paused.SetStopReason(sandbox.StopReasonPaused)
	assert.Equal(t, faultDomainNone, classifyExecutionEnd(paused, nil).faultDomain)

	regFailed := newSbx()
	regFailed.SetStopReason(sandbox.StopReasonRegistrationFailed)
	assert.Equal(t, faultDomainPlatform, classifyExecutionEnd(regFailed, nil).faultDomain)

	// The first kill reason wins, like the stop reason.
	killed := newSbx()
	killed.SetKillReason("timeout")
	killed.SetKillReason(killReasonSealFailed)
	killed.SetStopReason(sandbox.StopReasonKilled)
	end = classifyExecutionEnd(killed, nil)
	assert.Equal(t, "timeout", end.killReason)
	assert.Equal(t, faultDomainUser, end.faultDomain)

	// Killed with no recorded reason cannot be attributed.
	unattributed := newSbx()
	unattributed.SetStopReason(sandbox.StopReasonKilled)
	assert.Equal(t, faultDomainUnknown, classifyExecutionEnd(unattributed, nil).faultDomain)
}

// A Delete that ends a checkpoint publishes the kill on the original sandbox;
// the resumed one, with its own metadata, still records the reason.
func TestMarkResumedSandboxEndedRecordsKillReason(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(&deletedDuringCheckpointError{killReason: "timeout"})

	resumed := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
	markResumedSandboxEnded(ctx, resumed)

	end := classifyExecutionEnd(resumed, nil)
	assert.Equal(t, sandbox.StopReasonKilled, end.stopReason)
	assert.Equal(t, "timeout", end.killReason)
	assert.Equal(t, faultDomainUser, end.faultDomain)
}
