//go:build linux

package chroot

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/handlecache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type volumeReleaseFixture struct {
	handler   *NFSHandler
	handles   *handlecache.Handler
	builder   *chrooted.Builder
	sandboxes *sandbox.Map
	sbx       *sandbox.Sandbox
	remote    net.Addr
	volume    sandbox.VolumeMountConfig
	hostPath  string
}

func newVolumeReleaseFixture(t *testing.T) volumeReleaseFixture {
	t.Helper()

	teamID := uuid.New()
	volume := sandbox.VolumeMountConfig{ID: uuid.New(), Name: "volume", Path: "/mnt/volume", Type: "local"}
	builder := chrooted.NewBuilder(cfg.Config{PersistentVolumeMounts: map[string]string{"local": t.TempDir()}})
	hostPath, err := builder.BuildVolumePath(volume.Type, teamID, volume.ID)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(hostPath, 0o755))

	sandboxes := sandbox.NewSandboxesMap()
	handler, err := NewNFSHandler(builder, sandboxes)
	require.NoError(t, err)
	handles := handlecache.Wrap(handler, 16)
	handler.OnMount(handles.Register)
	handler.OnRelease(handles.Release)

	sbx := &sandbox.Sandbox{
		LifecycleID: uuid.NewString(),
		Metadata: &sandbox.Metadata{
			Config:  sandbox.NewConfig(sandbox.Config{VolumeMounts: []sandbox.VolumeMountConfig{volume}}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), TeamID: teamID.String()},
		},
		Resources: &sandbox.Resources{Slot: &network.Slot{Key: uuid.NewString(), HostIP: net.IPv4(127, 0, 0, 1)}},
	}
	sandboxes.AssignNetwork(t.Context(), sbx)
	t.Cleanup(func() { assert.NoError(t, handler.OnNetworkRelease(context.WithoutCancel(t.Context()), sbx)) })

	return volumeReleaseFixture{
		handler: handler, handles: handles, builder: builder, sandboxes: sandboxes,
		sbx: sbx, remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10000}, volume: volume, hostPath: hostPath,
	}
}

func countRootDescriptors(t *testing.T, root string) int {
	t.Helper()

	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	count := 0
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err == nil && target == root {
			count++
		}
	}

	return count
}

func requireStaleHandle(t *testing.T, handles *handlecache.Handler, handle []byte) {
	t.Helper()

	_, _, err := handles.FromHandle(t.Context(), handle)
	var status *nfs.NFSStatusError
	require.ErrorAs(t, err, &status)
	assert.Equal(t, nfs.NFSStatusStale, status.NFSStatus)
}

func TestVolumeMountRemovalClosesRootAndRetiresHandles(t *testing.T) {
	t.Parallel()

	f := newVolumeReleaseFixture(t)
	request := nfs.MountRequest{Dirpath: []byte("/" + f.volume.Name)}
	assert.Zero(t, countRootDescriptors(t, f.hostPath))
	fs, err := f.handler.getChroot(t.Context(), f.remote, request)
	require.NoError(t, err)
	before := f.handles.ToHandle(t.Context(), fs, nil)
	_, _, err = f.handles.FromHandle(t.Context(), before)
	require.NoError(t, err)
	assert.Equal(t, 1, countRootDescriptors(t, f.hostPath))

	f.sbx.Config = sandbox.NewConfig(sandbox.Config{})
	f.handler.OnVolumeMountsRemoved(t.Context(), f.sbx)

	_, cached := f.handler.mounted(f.sbx.LifecycleID, f.volume.ID)
	assert.False(t, cached)
	_, err = fs.Stat("/")
	require.ErrorIs(t, err, chrooted.ErrClosed)
	assert.Zero(t, countRootDescriptors(t, f.hostPath))
	requireStaleHandle(t, f.handles, before)

	// A retry opens a fresh filesystem and must not resurrect its old handles.
	f.sbx.Config = sandbox.NewConfig(sandbox.Config{VolumeMounts: []sandbox.VolumeMountConfig{f.volume}})
	again, err := f.handler.getChroot(t.Context(), f.remote, request)
	require.NoError(t, err)
	assert.NotSame(t, fs, again)
	after := f.handles.ToHandle(t.Context(), again, nil)
	assert.NotEqual(t, before, after)
	_, _, err = f.handles.FromHandle(t.Context(), after)
	require.NoError(t, err)
	requireStaleHandle(t, f.handles, before)
	assert.Equal(t, 1, countRootDescriptors(t, f.hostPath))
}

func TestVolumeMountRemovalKeepsVolumeAtAnotherPath(t *testing.T) {
	t.Parallel()

	f := newVolumeReleaseFixture(t)
	alias := f.volume
	alias.Path = "/mnt/another-path"
	f.sbx.Config = sandbox.NewConfig(sandbox.Config{VolumeMounts: []sandbox.VolumeMountConfig{f.volume, alias}})
	fs, err := f.handler.getChroot(t.Context(), f.remote, nfs.MountRequest{Dirpath: []byte("/" + f.volume.Name)})
	require.NoError(t, err)
	handle := f.handles.ToHandle(t.Context(), fs, nil)

	f.sbx.Config = sandbox.NewConfig(sandbox.Config{VolumeMounts: []sandbox.VolumeMountConfig{alias}})
	f.handler.OnVolumeMountsRemoved(t.Context(), f.sbx)

	retained, cached := f.handler.mounted(f.sbx.LifecycleID, f.volume.ID)
	require.True(t, cached)
	assert.Same(t, fs, retained)
	_, err = fs.Stat("/")
	require.NoError(t, err)
	_, _, err = f.handles.FromHandle(t.Context(), handle)
	require.NoError(t, err)
	assert.Equal(t, 1, countRootDescriptors(t, f.hostPath))
}

func TestDelayedMountCannotPublishAfterRemoval(t *testing.T) {
	t.Parallel()

	for _, change := range []string{"volume removed", "volume replaced", "type changed", "network released", "network reassigned"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()

			f := newVolumeReleaseFixture(t)
			opened, err := f.builder.Chroot(f.volume.Type, uuid.MustParse(f.sbx.Runtime.TeamID), f.volume.ID)
			require.NoError(t, err)
			t.Cleanup(func() { _ = opened.Close() })

			switch change {
			case "volume removed":
				f.sbx.Config = sandbox.NewConfig(sandbox.Config{})
				f.handler.OnVolumeMountsRemoved(t.Context(), f.sbx)
			case "volume replaced":
				replacement := f.volume
				replacement.ID = uuid.New()
				f.sbx.Config = sandbox.NewConfig(sandbox.Config{VolumeMounts: []sandbox.VolumeMountConfig{replacement}})
			case "type changed":
				replacement := f.volume
				replacement.Type = "other"
				f.sbx.Config = sandbox.NewConfig(sandbox.Config{VolumeMounts: []sandbox.VolumeMountConfig{replacement}})
			case "network released":
				require.NoError(t, f.sandboxes.NetworkReleased(t.Context(), f.sbx.Slot.HostIPString()))
			case "network reassigned":
				replacement := &sandbox.Sandbox{
					LifecycleID: uuid.NewString(),
					Metadata:    f.sbx.Metadata,
					Resources:   f.sbx.Resources,
				}
				f.sandboxes.AssignNetwork(t.Context(), replacement)
			}

			fs, err := f.handler.cacheChroot(t.Context(), f.remote, f.sbx, f.volume, opened)
			require.Error(t, err)
			assert.Nil(t, fs)
			_, cached := f.handler.mounted(f.sbx.LifecycleID, f.volume.ID)
			assert.False(t, cached)
			// The caller retains ownership of an unpublished root.
			_, err = opened.Stat("/")
			require.NoError(t, err)
			require.NoError(t, opened.Close())
			assert.Zero(t, countRootDescriptors(t, f.hostPath))
		})
	}
}

func TestMountRegistrationCompletesBeforeRelease(t *testing.T) {
	t.Parallel()

	f := newVolumeReleaseFixture(t)
	registering := make(chan struct{})
	resume := make(chan struct{})
	resumeOnce := sync.OnceFunc(func() { close(resume) })
	defer resumeOnce()
	f.handler.onMount = append([]func(billy.Filesystem){func(billy.Filesystem) {
		close(registering)
		<-resume
	}}, f.handler.onMount...)

	type mountResult struct {
		fs  *wrappedFS
		err error
	}
	mounted := make(chan mountResult, 1)
	go func() {
		fs, err := f.handler.getChroot(t.Context(), f.remote, nfs.MountRequest{Dirpath: []byte("/" + f.volume.Name)})
		mounted <- mountResult{fs: fs, err: err}
	}()
	select {
	case <-registering:
	case <-time.After(5 * time.Second):
		t.Fatal("mount did not reach registration")
	}

	releasing := make(chan struct{})
	released := make(chan error, 1)
	go func() {
		close(releasing)
		released <- f.handler.OnNetworkRelease(t.Context(), f.sbx)
	}()
	<-releasing
	select {
	case err := <-released:
		t.Fatalf("release overtook mount registration: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	resumeOnce()

	var result mountResult
	select {
	case result = <-mounted:
		require.NoError(t, result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("mount did not finish")
	}
	select {
	case err := <-released:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("release did not finish")
	}
	assert.Zero(t, countRootDescriptors(t, f.hostPath))
	_, err := result.fs.Stat("/")
	require.ErrorIs(t, err, chrooted.ErrClosed)
	// A response completing after release cannot retain usable handles.
	handle := f.handles.ToHandle(t.Context(), result.fs, nil)
	requireStaleHandle(t, f.handles, handle)
}
