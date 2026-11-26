package sandbox

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func newVolumeMountSandbox(t *testing.T, handler http.HandlerFunc, existing ...VolumeMountConfig) *Sandbox {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	token := "test-token"
	s := &Sandbox{
		Metadata: &Metadata{
			Config:  NewConfig(Config{VolumeMounts: existing, Envd: EnvdMetadata{AccessToken: &token}}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "test-sandbox"},
		},
		LifecycleID: "lc-1",
	}
	s.internalConfig.envdServerURLOverride = server.URL

	return s
}

func volumeMount(name, path string) VolumeMountConfig {
	return VolumeMountConfig{ID: uuid.NewSHA1(uuid.Nil, []byte(name)), Name: name, Path: path, Type: "local"}
}

func TestAppendVolumeMountsSendsNewMountsToEnvd(t *testing.T) {
	t.Parallel()

	var got envd.PostVolumesMountJSONBody
	s := newVolumeMountSandbox(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/volumes/mount", r.URL.Path)
		assert.Equal(t, "test-token", r.Header.Get("X-Access-Token"))
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.WriteHeader(http.StatusNoContent)
	}, volumeMount("a", "/mnt/a"))

	require.NoError(t, s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")}))

	assert.Equal(t, "lc-1", got.LifecycleID)
	require.Len(t, got.VolumeMounts, 1)
	assert.Equal(t, "/mnt/b", got.VolumeMounts[0].Path)
	assert.Equal(t, []VolumeMountConfig{volumeMount("a", "/mnt/a"), volumeMount("b", "/mnt/b")}, s.Config.GetVolumeMounts())
}

func TestAppendVolumeMountsRetryKeepsOneEntry(t *testing.T) {
	t.Parallel()

	s := newVolumeMountSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mounts := []VolumeMountConfig{volumeMount("b", "/mnt/b")}

	require.NoError(t, s.AppendVolumeMounts(t.Context(), mounts))
	require.NoError(t, s.AppendVolumeMounts(t.Context(), mounts))
	assert.Len(t, s.Config.GetVolumeMounts(), 1)
}

func TestAppendVolumeMountsRollsBackOnEnvdFailure(t *testing.T) {
	t.Parallel()

	s := newVolumeMountSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(envdFailedVolumeHeader, "/mnt/b")
		w.WriteHeader(http.StatusBadRequest)
	}, volumeMount("a", "/mnt/a"))

	err := s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")})

	var volumeErr VolumeMountError
	require.ErrorAs(t, err, &volumeErr)
	assert.Equal(t, "/mnt/b", volumeErr.Path)
	assert.Equal(t, []VolumeMountConfig{volumeMount("a", "/mnt/a")}, s.Config.GetVolumeMounts())
}

func TestAppendVolumeMountsOnOldEnvd(t *testing.T) {
	t.Parallel()

	s := newVolumeMountSandbox(t, http.NotFound)

	err := s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")})
	require.ErrorIs(t, err, ErrEnvdVolumeAppendUnsupported)
	assert.Empty(t, s.Config.GetVolumeMounts())
}

func TestAppendVolumeMountsRejectsConflictsWithoutCallingEnvd(t *testing.T) {
	t.Parallel()

	tests := map[string][]VolumeMountConfig{
		"same path, other volume": {volumeMount("b", "/mnt/a")},
		"inside an existing":      {volumeMount("b", "/mnt/a/sub")},
		"over an existing":        {volumeMount("b", "/mnt")},
		"overlap in the request":  {volumeMount("b", "/data"), volumeMount("c", "/data/sub")},
		// A volume recreated under a deleted one's name has a new ID.
		"same name, other volume": {{ID: uuid.New(), Name: "a", Path: "/mnt/other", Type: "local"}},
	}

	for name, mounts := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := newVolumeMountSandbox(t, func(http.ResponseWriter, *http.Request) {
				t.Error("envd must not be called")
			}, volumeMount("a", "/mnt/a"))

			require.ErrorIs(t, s.AppendVolumeMounts(t.Context(), mounts), ErrVolumeMountConflict)
			assert.Len(t, s.Config.GetVolumeMounts(), 1)
		})
	}
}

func TestAppendVolumeMountsRejectsRetryForPreExistingSameNameConflict(t *testing.T) {
	t.Parallel()

	existing := []VolumeMountConfig{
		volumeMount("a", "/mnt/a"),
		{ID: uuid.New(), Name: "a", Path: "/mnt/other", Type: "local"},
	}
	s := newVolumeMountSandbox(t, func(http.ResponseWriter, *http.Request) {
		t.Error("envd must not be called")
	}, existing...)

	err := s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{existing[0]})
	require.ErrorIs(t, err, ErrVolumeMountConflict)
	assert.Equal(t, existing, s.Config.GetVolumeMounts())
}

func TestAppendVolumeMountsRefusedOnceStopping(t *testing.T) {
	t.Parallel()

	s := newVolumeMountSandbox(t, func(http.ResponseWriter, *http.Request) {
		t.Error("envd must not be called")
	})
	s.updateAdmission.closeAndSnapshotActive()

	require.ErrorIs(t, s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")}), ErrSandboxUpdateClosed)
}

func TestPathsOverlap(t *testing.T) {
	t.Parallel()

	assert.True(t, pathsOverlap("/data", "/data"))
	assert.True(t, pathsOverlap("/data", "/data/x"))
	assert.True(t, pathsOverlap("/data/x", "/data"))
	assert.False(t, pathsOverlap("/data", "/database"))
	assert.False(t, pathsOverlap("/data/a", "/data/b"))
}

func TestAppendVolumeMountsKeepsTheConfigWhenEnvdDoesNotAnswer(t *testing.T) {
	t.Parallel()

	s := newVolumeMountSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
		// Drop the connection without a status: the outcome is unknown.
		conn, _, err := w.(http.Hijacker).Hijack()
		if assert.NoError(t, err) {
			conn.Close()
		}
	})

	require.ErrorIs(t, s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")}), ErrVolumeMountUnsettled)
	assert.Equal(t, []VolumeMountConfig{volumeMount("b", "/mnt/b")}, s.Config.GetVolumeMounts(),
		"the guest may hold the mount, so it stays listed")

	err := s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("c", "/mnt/b")})
	require.ErrorIs(t, err, ErrVolumeMountConflict, "another volume at the same path is refused, not skipped by envd")
}

func TestAppendVolumeMountsOutlivesItsCaller(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	s := newVolumeMountSandbox(t, func(w http.ResponseWriter, r *http.Request) {
		cancel()
		assert.NoError(t, r.Context().Err(), "the caller going away does not end the envd call")
		w.WriteHeader(http.StatusNoContent)
	})

	require.NoError(t, s.AppendVolumeMounts(ctx, []VolumeMountConfig{volumeMount("b", "/mnt/b")}))
}

func TestAppendVolumeMountsExcludesCheckpoints(t *testing.T) {
	t.Parallel()

	var s *Sandbox
	s = newVolumeMountSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
		assert.False(t, s.BeginCheckpoint(), "a checkpoint cannot start while envd mounts")
		w.WriteHeader(http.StatusNoContent)
	})

	require.NoError(t, s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")}))
	require.True(t, s.BeginCheckpoint(), "the marker is released after the append")

	err := s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("c", "/mnt/c")})
	require.ErrorIs(t, err, ErrCheckpointInFlight)
	s.EndCheckpoint()
}

func TestAppendVolumeMountsKeepsTheConfigWhenEnvdCouldNotRollBack(t *testing.T) {
	t.Parallel()

	s := newVolumeMountSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(envdFailedVolumeHeader, "/mnt/c")
		w.WriteHeader(http.StatusInternalServerError)
	})

	err := s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b"), volumeMount("c", "/mnt/c")})
	require.ErrorIs(t, err, ErrVolumeMountUnsettled, "not a refusal: some mounts may be in place")
	assert.Len(t, s.Config.GetVolumeMounts(), 2, "a mount envd could not detach keeps the NFS proxy serving it")
}

// unansweredThen drops the first envd call without a status, as a lost
// response, and answers every later one with status, recording each body.
func unansweredThen(t *testing.T, status int, bodies *[]envd.PostVolumesMountJSONBody) http.HandlerFunc {
	t.Helper()

	return func(w http.ResponseWriter, r *http.Request) {
		var body envd.PostVolumesMountJSONBody
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		*bodies = append(*bodies, body)
		if len(*bodies) == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if assert.NoError(t, err) {
				conn.Close()
			}

			return
		}
		w.WriteHeader(status)
	}
}

func TestAppendVolumeMountsDropsAnUnsettledMountEnvdThenRefuses(t *testing.T) {
	t.Parallel()

	var bodies []envd.PostVolumesMountJSONBody
	s := newVolumeMountSandbox(t, unansweredThen(t, http.StatusBadRequest, &bodies))
	mounts := []VolumeMountConfig{volumeMount("b", "/mnt/b")}

	require.ErrorIs(t, s.AppendVolumeMounts(t.Context(), mounts), ErrVolumeMountUnsettled)
	require.Len(t, s.Config.GetVolumeMounts(), 1)

	require.Error(t, s.AppendVolumeMounts(t.Context(), mounts))
	require.Len(t, bodies, 2)
	assert.Empty(t, bodies[0].UnsettledPaths)
	assert.Equal(t, []string{"/mnt/b"}, bodies[1].UnsettledPaths, "the retry asks envd to settle the path")
	assert.Empty(t, s.Config.GetVolumeMounts(), "envd answered nothing is left, so the config drops it")
}

func TestAppendVolumeMountsSettlesAnUnsettledMountOnSuccess(t *testing.T) {
	t.Parallel()

	var bodies []envd.PostVolumesMountJSONBody
	s := newVolumeMountSandbox(t, unansweredThen(t, http.StatusNoContent, &bodies))
	mounts := []VolumeMountConfig{volumeMount("b", "/mnt/b")}

	require.ErrorIs(t, s.AppendVolumeMounts(t.Context(), mounts), ErrVolumeMountUnsettled)
	require.NoError(t, s.AppendVolumeMounts(t.Context(), mounts))
	require.NoError(t, s.AppendVolumeMounts(t.Context(), mounts))

	require.Len(t, bodies, 3)
	assert.Equal(t, []string{"/mnt/b"}, bodies[1].UnsettledPaths)
	assert.Empty(t, bodies[2].UnsettledPaths, "settled by the success")
	assert.Len(t, s.Config.GetVolumeMounts(), 1)
}

func TestUnsettledVolumeMountsAreLeftOutOfInit(t *testing.T) {
	t.Parallel()

	var bodies []envd.PostVolumesMountJSONBody
	s := newVolumeMountSandbox(t, unansweredThen(t, http.StatusNoContent, &bodies), volumeMount("a", "/mnt/a"))
	require.ErrorIs(t, s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")}), ErrVolumeMountUnsettled)

	assert.Len(t, s.Config.GetVolumeMounts(), 2, "listed, so the NFS proxy serves what the guest may hold")
	assert.Equal(t, []VolumeMountConfig{volumeMount("a", "/mnt/a")}, s.Config.GetSettledVolumeMounts(), "no /init asks envd to mount it")
	assert.Equal(t, []VolumeMountConfig{volumeMount("a", "/mnt/a")}, s.Config.Clone().GetSettledVolumeMounts(), "a resume-fresh checkpoint copies the mark")

	require.NoError(t, s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")}))
	assert.Len(t, s.Config.GetSettledVolumeMounts(), 2, "settled by the retry")
}

func TestAppendVolumeMountsTreatsOnlyA400AsSettled(t *testing.T) {
	t.Parallel()

	s := newVolumeMountSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})

	require.ErrorIs(t, s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")}), ErrVolumeMountUnsettled)
	assert.Len(t, s.Config.GetVolumeMounts(), 1)
}

type volumeMountRemovalSubscriber struct {
	onRemoved func(context.Context, *Sandbox)
}

func (volumeMountRemovalSubscriber) OnInsert(context.Context, *Sandbox) {}

func (volumeMountRemovalSubscriber) OnStopping(context.Context, *Sandbox) {}

func (volumeMountRemovalSubscriber) OnNetworkRelease(context.Context, *Sandbox) error {
	return nil
}

func (s volumeMountRemovalSubscriber) OnVolumeMountsRemoved(ctx context.Context, sbx *Sandbox) {
	s.onRemoved(ctx, sbx)
}

func TestAppendVolumeMountsReleasesRemovedMountsWithinUpdate(t *testing.T) {
	t.Parallel()

	existing := volumeMount("a", "/mnt/a")
	s := newVolumeMountSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}, existing)
	s.sandboxes = NewSandboxesMap()
	removed := false
	s.sandboxes.Subscribe(volumeMountRemovalSubscriber{onRemoved: func(_ context.Context, sbx *Sandbox) {
		removed = true
		assert.Same(t, s, sbx)
		assert.Equal(t, []VolumeMountConfig{existing}, sbx.Config.GetVolumeMounts(), "cleanup sees only the mounts that still need their resources")
		assert.False(t, sbx.BeginCheckpoint(), "the checkpoint marker stays held until resources are released")
		unlocked := sbx.updateMu.TryLock()
		if unlocked {
			sbx.updateMu.Unlock()
		}
		assert.False(t, unlocked, "a retry cannot begin while rollback releases its resources")
	}})

	require.Error(t, s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")}))
	require.True(t, removed, "a confirmed rollback releases resources before returning")
	require.True(t, s.BeginCheckpoint())
	s.EndCheckpoint()
}

func TestAppendVolumeMountsReleasesUnsettledMountsOnlyAfterConfirmedRollback(t *testing.T) {
	t.Parallel()

	var bodies []envd.PostVolumesMountJSONBody
	s := newVolumeMountSandbox(t, unansweredThen(t, http.StatusBadRequest, &bodies))
	s.sandboxes = NewSandboxesMap()
	mounts := []VolumeMountConfig{volumeMount("b", "/mnt/b")}
	removals := 0
	s.sandboxes.Subscribe(volumeMountRemovalSubscriber{onRemoved: func(_ context.Context, sbx *Sandbox) {
		removals++
		assert.Empty(t, sbx.Config.GetVolumeMounts())
		assert.Empty(t, sbx.Config.unsettledAmong(mounts), "the unknown outcome is settled before resources are released")
	}})

	require.ErrorIs(t, s.AppendVolumeMounts(t.Context(), mounts), ErrVolumeMountUnsettled)
	assert.Zero(t, removals, "a lost response leaves the guest's mount resources available")

	require.Error(t, s.AppendVolumeMounts(t.Context(), mounts))
	assert.Equal(t, 1, removals, "the retry confirmed that the guest released the mount")
}

func TestAppendVolumeMountsKeepsResourcesForSuccessfulOrUnsettledMounts(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusNoContent, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()

			s := newVolumeMountSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			})
			s.sandboxes = NewSandboxesMap()
			s.sandboxes.Subscribe(volumeMountRemovalSubscriber{onRemoved: func(context.Context, *Sandbox) {
				t.Error("the guest may still use these mount resources")
			}})

			err := s.AppendVolumeMounts(t.Context(), []VolumeMountConfig{volumeMount("b", "/mnt/b")})
			if status == http.StatusNoContent {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrVolumeMountUnsettled)
			}
			assert.Len(t, s.Config.GetVolumeMounts(), 1)
		})
	}
}
