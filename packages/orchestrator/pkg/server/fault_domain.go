package server

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
)

// faultDomain says whose side an ended execution points at, so a failed
// sandbox can be triaged as a usage problem or a service problem without
// reading every signal. It is a metric label, so the set stays small and closed.
type faultDomain string

const (
	// faultDomainNone: the execution ended as intended (pause, checkpoint).
	faultDomainNone faultDomain = "none"
	// faultDomainUser: the user ended it (kill, timeout, team deletion) or the
	// guest ran out of memory before it went down.
	faultDomainUser faultDomain = "user"
	// faultDomainPlatform: the service ended it, by a failed operation, an
	// operator, or a host-side crash.
	faultDomainPlatform faultDomain = "platform"
	// faultDomainUnknown: the signals do not tell, e.g. a guest that shut
	// itself down with no OOM kill seen.
	faultDomainUnknown faultDomain = "unknown"
)

func faultDomainAttr(d faultDomain) attribute.KeyValue {
	return attribute.String("fault_domain", string(d))
}

// killFaultDomain classifies a kill by its reason. The API's reasons are in
// packages/api/internal/sandbox/sandboxtypes/states.go; the orchestrator's own
// are the killReason* constants.
func killFaultDomain(killReason string) faultDomain {
	switch killReason {
	case "request", "timeout", "team_deleted":
		return faultDomainUser
	case "admin", "orphaned", "base_template_missing",
		killReasonSealFailed, killReasonResumeFailed, killReasonThawFailed, killReasonUploadFailed:
		return faultDomainPlatform
	default:
		return faultDomainUnknown
	}
}

// crashFaultDomain classifies an execution that ended with no stop reason.
// A guest that halted itself is the user's only when the guest OOM killer ran;
// a clean exit is otherwise also how a guest kernel panic looks.
func crashFaultDomain(cause fc.CrashCause, guestOOMKills int) faultDomain {
	switch cause {
	case fc.CrashCauseCleanExit:
		if guestOOMKills > 0 {
			return faultDomainUser
		}

		return faultDomainUnknown
	case fc.CrashCauseExitError, fc.CrashCauseExternalSignal, fc.CrashCauseFault,
		fc.CrashCauseMemoryHandlerFailed, fc.CrashCauseRequestedSignal:
		return faultDomainPlatform
	default:
		return faultDomainUnknown
	}
}

// executionEnd is what is known about an execution once it has stopped.
type executionEnd struct {
	stopReason    sandbox.StopReason
	killReason    string
	crashCause    fc.CrashCause // set only when stopReason is crashed
	uffdCopyError uffdCopyError // set only when stopReason is crashed
	guestOOMKills int
	faultDomain   faultDomain
}

func classifyExecutionEnd(sbx *sandbox.Sandbox, waitErr error) executionEnd {
	end := executionEnd{
		stopReason:    sbx.GetStopReason(),
		killReason:    sbx.GetKillReason(),
		guestOOMKills: sbx.OOMKills.Observed(),
	}

	switch end.stopReason {
	case sandbox.StopReasonPaused, sandbox.StopReasonCheckpointing:
		end.faultDomain = faultDomainNone
	case sandbox.StopReasonRegistrationFailed:
		end.faultDomain = faultDomainPlatform
	case sandbox.StopReasonKilled:
		end.faultDomain = killFaultDomain(end.killReason)
	case sandbox.StopReasonCrashed:
		memoryHandlerErr := sbx.MemoryHandlerErr()
		end.crashCause = crashCause(sbx.FirecrackerExit(), memoryHandlerErr)
		end.uffdCopyError = classifyUffdCopyError(errors.Join(waitErr, memoryHandlerErr))
		end.faultDomain = crashFaultDomain(end.crashCause, end.guestOOMKills)
	default:
		end.faultDomain = faultDomainUnknown
	}

	return end
}

// logExecutionEnd writes one line per ended execution with every signal the
// triage needs, so a sandbox ID is enough to find out why it went down.
func (s *Server) logExecutionEnd(ctx context.Context, sbx *sandbox.Sandbox, end executionEnd) {
	fields := []zap.Field{
		zap.String("stop_reason", string(end.stopReason)),
		zap.String("fault_domain", string(end.faultDomain)),
		zap.Int("guest_oom_kills", end.guestOOMKills),
	}
	if end.killReason != "" {
		fields = append(fields, zap.String("kill_reason", end.killReason))
	}
	if end.stopReason == sandbox.StopReasonCrashed {
		fields = append(fields,
			zap.String("crash_cause", string(end.crashCause)),
			zap.String("uffd_copy_error", string(end.uffdCopyError)),
		)
	}
	if duration, ok := sbx.ExecutionDuration(); ok {
		fields = append(fields, zap.Int64("execution_duration_ms", duration.Milliseconds()))
	}
	if s.hostMetrics != nil {
		if memory, err := s.hostMetrics.GetMemoryMetrics(ctx); err == nil {
			fields = append(fields,
				zap.Uint64("node_hugepages_total", memory.HugePagesTotal),
				zap.Uint64("node_hugepages_used", memory.HugePagesUsed),
				zap.Uint64("node_hugepages_reserved", memory.HugePagesReserved),
			)
		}
	}

	sbxlogger.I(sbx).Info(ctx, "sandbox execution ended", fields...)
}
