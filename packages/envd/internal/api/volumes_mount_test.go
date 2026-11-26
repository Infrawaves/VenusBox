package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func newVolumesMountTestAPI(t *testing.T) (*API, *fakeNFS, string) {
	t.Helper()

	a, fake, root := newNFSTestAPI(t)
	logger := zerolog.Nop()
	a.logger = &logger
	a.initLock = semaphore.NewWeighted(1)

	return a, fake, root
}

func postVolumesMount(t *testing.T, a *API, body any) *httptest.ResponseRecorder {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	a.PostVolumesMount(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/volumes/mount", bytes.NewReader(raw)))

	return rec
}

func TestPostVolumesMountMountsNextToInitVolumes(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	initMounts := volumes(root, "a")
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), initMounts))

	appended := volumes(root, "b/sub", "b")
	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended})
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())

	assert.Equal(t, []string{initMounts[0].Path, appended[1].Path, appended[0].Path}, fake.mounted, "parents before children")

	// The next /init lists every volume and must not remount any for the same lifecycle.
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), append(initMounts, appended...)))
	assert.Len(t, fake.mounted, 3)
}

func TestPostVolumesMountRetryIsANoop(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	body := PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: volumes(root, "a")}

	require.Equal(t, http.StatusNoContent, postVolumesMount(t, a, body).Code)
	require.Equal(t, http.StatusNoContent, postVolumesMount(t, a, body).Code)
	assert.Len(t, fake.mounted, 1)
}

func TestPostVolumesMountRollsBackAndDoesNotPoisonInit(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	initMounts := volumes(root, "a")
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), initMounts))

	appended := volumes(root, "b", "c")
	fake.failing[appended[1].Path] = errors.New("access denied by server")

	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, appended[1].Path, rec.Header().Get(failedVolumeHeader))
	assert.Contains(t, fake.ops, "unmount "+appended[0].Path, "the volume mounted before the failure is rolled back")

	for _, volume := range appended {
		_, recorded := a.mountedPaths.Load(volume.Path)
		assert.False(t, recorded, volume.Path)
	}
	_, kept := a.mountedPaths.Load(initMounts[0].Path)
	assert.True(t, kept, "the volumes /init mounted stay")

	// A reinit of the same lifecycle with the original list still succeeds.
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), initMounts))
}

func TestPostVolumesMountRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	tests := map[string][]VolumeMount{
		"empty":          {},
		"relative path":  {{NfsTarget: "127.0.0.1:/v", Path: "data"}},
		"root":           {{NfsTarget: "127.0.0.1:/v", Path: "/"}},
		"unclean path":   {{NfsTarget: "127.0.0.1:/v", Path: "/data/../etc"}},
		"no target":      {{Path: "/data"}},
		"duplicate path": {{NfsTarget: "127.0.0.1:/v", Path: "/data"}, {NfsTarget: "127.0.0.1:/w", Path: "/data"}},
	}

	for name, mounts := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			a, fake, _ := newVolumesMountTestAPI(t)
			rec := postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: mounts})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Empty(t, fake.mounted)
		})
	}
}

func TestPostVolumesMountRefusesToHideAMountedVolume(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	initMounts := volumes(root, "data")
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), initMounts))

	// alias is a symlink to data in the guest: mounting it covers data's mount.
	appended := volumes(root, "alias")
	fake.covers[appended[0].Path] = initMounts[0].Path

	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, appended[0].Path, rec.Header().Get(failedVolumeHeader))
	assert.Contains(t, rec.Body.String(), "would hide the volume mounted at")
	assert.Contains(t, fake.ops, "unmount "+appended[0].Path, "the covering mount is rolled back")

	_, recorded := a.mountedPaths.Load(appended[0].Path)
	assert.False(t, recorded)
	_, kept := a.mountedPaths.Load(initMounts[0].Path)
	assert.True(t, kept)
}

func TestUnmountUnlistedNFSDetachesStaleMountsOnly(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	mounts := volumes(root, "listed", "appended", "appended/child")
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), mounts))
	setupOps := len(fake.ops)

	// The same lifecycle, as in a reinit: nothing is stale.
	require.NoError(t, a.unmountUnlistedNFS(zerolog.Nop(), new("lc-1"), &[]VolumeMount{mounts[0]}))
	// No lifecycle: staleness cannot be told.
	require.NoError(t, a.unmountUnlistedNFS(zerolog.Nop(), nil, &[]VolumeMount{mounts[0]}))
	assert.Len(t, fake.ops, setupOps, "nothing is detached")

	// A resume that lists only the first volume.
	require.NoError(t, a.unmountUnlistedNFS(zerolog.Nop(), new("lc-2"), &[]VolumeMount{mounts[0]}))
	assert.Equal(t, []string{"unmount " + mounts[2].Path, "unmount " + mounts[1].Path}, fake.ops[setupOps:], "children first")

	for _, volume := range mounts[1:] {
		_, recorded := a.mountedPaths.Load(volume.Path)
		assert.False(t, recorded, volume.Path)
	}
	_, kept := a.mountedPaths.Load(mounts[0].Path)
	assert.True(t, kept, "a listed volume is left to setupNFS")
}

func TestUnmountUnlistedNFSWithNoVolumesListed(t *testing.T) {
	t.Parallel()

	a, _, root := newVolumesMountTestAPI(t)
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), volumes(root, "a")))

	// The orchestrator omits an empty volumeMounts.
	require.NoError(t, a.unmountUnlistedNFS(zerolog.Nop(), new("lc-2"), nil))

	count := 0
	a.mountedPaths.Range(func(any, any) bool {
		count++

		return true
	})
	assert.Zero(t, count)
}

func TestPostVolumesMountKeepsAMountItCannotRollBack(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	appended := volumes(root, "b", "c")
	fake.failing[appended[1].Path] = errors.New("access denied by server")
	fake.unmountFailing[appended[0].Path] = errors.New("device busy")

	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended})
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "the orchestrator must keep serving what is still mounted")
	assert.Equal(t, appended[1].Path, rec.Header().Get(failedVolumeHeader))

	value, recorded := a.mountedPaths.Load(appended[0].Path)
	require.True(t, recorded, "the mount with an uncertain cleanup stays in the ledger")
	assert.Empty(t, value, "an empty lifecycle marks the mount as unsettled")

	// A retry while it still cannot be detached is refused the same way.
	delete(fake.failing, appended[1].Path)
	rec = postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended})
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	// Once it can be detached, the retry detaches it and mounts afresh.
	delete(fake.unmountFailing, appended[0].Path)
	mountsBefore := len(fake.mounted)
	rec = postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended})
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	assert.Len(t, fake.mounted, mountsBefore+2, "both volumes are mounted, none skipped")
	_, recorded = a.mountedPaths.Load(appended[0].Path)
	assert.True(t, recorded)
}

func TestPostVolumesMountRollsBackAMountWhoseIDCannotBeRead(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	appended := volumes(root, "b")
	fake.idLostOnMount[appended[0].Path] = true

	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "the mount is detached, so nothing is left")
	assert.Equal(t, "unmount "+appended[0].Path, fake.ops[len(fake.ops)-1], "the mount that may be in place is detached")

	_, recorded := a.mountedPaths.Load(appended[0].Path)
	assert.False(t, recorded)
}

func TestPostVolumesMountRetriesAfterTransientMountIDFailure(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	appended := volumes(root, "b")
	fake.mountIDErrorsAfterMount[appended[0].Path] = 1
	body := PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended}

	rec := postVolumesMount(t, a, body)
	require.Equal(t, http.StatusBadRequest, rec.Code, "the confirmed mount is rolled back")
	assert.Equal(t, []string{"mount " + appended[0].Path, "unmount " + appended[0].Path}, fake.ops)

	rec = postVolumesMount(t, a, body)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	assert.Equal(t, []string{"mount " + appended[0].Path, "unmount " + appended[0].Path, "mount " + appended[0].Path}, fake.ops)
}

func TestPostVolumesMountDoesNotSucceedWhenTimeoutAndMountIDReadFail(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	appended := volumes(root, "b")
	fake.timeouts[appended[0].Path] = nfsMountAttempts
	fake.mountIDErrorsAfterMount[appended[0].Path] = 1
	body := PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended}

	rec := postVolumesMount(t, a, body)
	require.Equal(t, http.StatusBadRequest, rec.Code, "the failed mount was not left in place")

	rec = postVolumesMount(t, a, body)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	assert.Len(t, fake.mounted, 3, "two timed-out attempts, then a successful retry")
}

func TestPostVolumesMountKeepsTimedOutMountWhenBothUnmountsFail(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	appended := volumes(root, "b")
	fake.timeouts[appended[0].Path] = 1
	fake.timeoutMounts[appended[0].Path] = 1
	fake.unmountFailing[appended[0].Path] = errors.New("device busy")
	body := PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended}

	rec := postVolumesMount(t, a, body)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	value, recorded := a.mountedPaths.Load(appended[0].Path)
	require.True(t, recorded)
	assert.Empty(t, value)

	// The retry must not report success or replace the unknown mount while it
	// still cannot detach it.
	rec = postVolumesMount(t, a, body)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	delete(fake.unmountFailing, appended[0].Path)
	rec = postVolumesMount(t, a, body)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	assert.Equal(t, []string{
		"mount " + appended[0].Path,
		"unmount " + appended[0].Path,
		"unmount " + appended[0].Path,
		"unmount " + appended[0].Path,
		"unmount " + appended[0].Path,
		"mount " + appended[0].Path,
	}, fake.ops)
}

func TestPostVolumesMountRejectsASymlinkPath(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	target := filepath.Join(root, "target")
	require.NoError(t, os.MkdirAll(target, 0o755))
	link := filepath.Join(root, "link")
	require.NoError(t, os.Symlink(target, link))

	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: []VolumeMount{{NfsTarget: "127.0.0.1:/v", Path: link}}})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "the path is a symlink")
	assert.Empty(t, fake.mounted)
}

func TestUnmountUnlistedNFSDetachesUnsettledMountsEvenWhenListed(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	mounts := volumes(root, "b")
	a.mountedPaths.Store(mounts[0].Path, "")

	// An empty lifecycle marks an unsettled mount, so /init detaches it even
	// when the path remains listed for this lifecycle.
	require.NoError(t, a.unmountUnlistedNFS(zerolog.Nop(), new("lc-1"), &mounts))
	assert.Equal(t, []string{"unmount " + mounts[0].Path}, fake.ops)
	_, left := a.mountedPaths.Load(mounts[0].Path)
	assert.False(t, left)

	// It is already gone when a later lifecycle starts.
	require.NoError(t, a.unmountUnlistedNFS(zerolog.Nop(), new("lc-2"), &mounts))
	assert.Len(t, fake.ops, 1)
}

func TestUnmountUnlistedNFSFailsWhenAStaleMountStays(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	mounts := volumes(root, "a")
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), mounts))
	fake.unmountFailing[mounts[0].Path] = errors.New("device busy")

	err := a.unmountUnlistedNFS(zerolog.Nop(), new("lc-2"), nil)
	var volumeErr *nfsVolumeError
	require.ErrorAs(t, err, &volumeErr)
	assert.Equal(t, mounts[0].Path, volumeErr.path)

	_, recorded := a.mountedPaths.Load(mounts[0].Path)
	assert.True(t, recorded, "a retried /init tries again")
}

func TestPostVolumesMountKeepsAVerifiedUnsettledPath(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	appended := volumes(root, "b")
	body := PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended}

	// The first call mounts, but its response is lost.
	require.Equal(t, http.StatusNoContent, postVolumesMount(t, a, body).Code)
	opsBefore := len(fake.ops)

	// The ledger has it for the lifecycle, so it is in place: not remounted.
	body.UnsettledPaths = &[]string{appended[0].Path}
	require.Equal(t, http.StatusNoContent, postVolumesMount(t, a, body).Code)
	assert.Len(t, fake.ops, opsBefore, "a remount would abort the volume's I/O")
}

func TestPostVolumesMountRemountsAnUnrecordedUnsettledPath(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	appended := volumes(root, "b")

	// envd never recorded a mount here, but an earlier call left one.
	require.NoError(t, os.MkdirAll(appended[0].Path, 0o755))
	fake.ids[appended[0].Path] = 999
	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{
		LifecycleID: new("lc-1"), VolumeMounts: appended, UnsettledPaths: &[]string{appended[0].Path},
	})
	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, []string{"unmount " + appended[0].Path, "mount " + appended[0].Path}, fake.ops)
}

func TestPostVolumesMountSettlesUnsettledPathsBeforeRefusing(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	known := volumes(root, "b")
	require.Equal(t, http.StatusNoContent, postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: known}).Code)
	unrecorded := volumes(root, "c")
	require.NoError(t, os.MkdirAll(unrecorded[0].Path, 0o755))
	fake.ids[unrecorded[0].Path] = 999 // left by an earlier call, unrecorded

	target := filepath.Join(root, "target")
	require.NoError(t, os.MkdirAll(target, 0o755))
	link := filepath.Join(root, "link")
	require.NoError(t, os.Symlink(target, link))

	// The new path is a symlink, refused before anything is mounted.
	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{
		LifecycleID:    new("lc-1"),
		VolumeMounts:   []VolumeMount{known[0], unrecorded[0], {NfsTarget: "127.0.0.1:/v", Path: link}},
		UnsettledPaths: &[]string{known[0].Path, unrecorded[0].Path},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, fake.ops, "unmount "+known[0].Path, "the verified unsettled mount is rolled back")
	assert.Contains(t, fake.ops, "unmount "+unrecorded[0].Path, "the unrecorded one is detached")
	_, recorded := a.mountedPaths.Load(known[0].Path)
	assert.False(t, recorded, "after a 400 nothing listed as unsettled is mounted")
}

func TestPostVolumesMountUnsettledSymlinkIsLeftMounted(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	target := filepath.Join(root, "target")
	require.NoError(t, os.MkdirAll(target, 0o755))
	link := filepath.Join(root, "link")
	require.NoError(t, os.Symlink(target, link))

	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{
		LifecycleID: new("lc-1"), VolumeMounts: []VolumeMount{{NfsTarget: "127.0.0.1:/v", Path: link}}, UnsettledPaths: &[]string{link},
	})
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "its state cannot be settled, so the orchestrator keeps it")
	assert.Empty(t, fake.ops)
}

func TestPostVolumesMountInvalidRequestWithUnsettledPathsIsA500(t *testing.T) {
	t.Parallel()

	a, _, root := newVolumesMountTestAPI(t)
	mounts := volumes(root, "b")
	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{
		LifecycleID: new("lc-1"), VolumeMounts: append(mounts, mounts[0]), UnsettledPaths: &[]string{mounts[0].Path},
	})
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "a refusal settles nothing")
}

func TestPostVolumesMountRollsBackAnUnsettledPathWithTheRest(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	appended := volumes(root, "b", "c")
	require.Equal(t, http.StatusNoContent, postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: appended[:1]}).Code)

	// The retry adds a volume that fails: the unsettled one must not stay behind.
	fake.failing[appended[1].Path] = errors.New("access denied by server")
	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{
		LifecycleID: new("lc-1"), VolumeMounts: appended, UnsettledPaths: &[]string{appended[0].Path},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "unmount "+appended[0].Path, fake.ops[len(fake.ops)-1], "rolled back with the rest")

	_, recorded := a.mountedPaths.Load(appended[0].Path)
	assert.False(t, recorded, "after a 400 the unsettled path is not mounted")
}

func TestPostVolumesMountRejectsAnUnlistedUnsettledPath(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{
		LifecycleID: new("lc-1"), VolumeMounts: volumes(root, "b"), UnsettledPaths: &[]string{"/elsewhere"},
	})
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "a refusal settles nothing")
	assert.Empty(t, fake.mounted)
}

func TestPostVolumesMountDoesNotDetachARecordedVolumeThroughASymlinkedParent(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	data := volumes(root, "data")
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), data))
	setupOps := len(fake.ops)

	// /alias -> root, so /alias/data resolves to the recorded /data mount.
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(root, alias))
	aliasData := filepath.Join(alias, "data")

	postVolumesMount(t, a, PostVolumesMountJSONBody{
		LifecycleID:    new("lc-1"),
		VolumeMounts:   []VolumeMount{{NfsTarget: "127.0.0.1:/other", Path: aliasData}},
		UnsettledPaths: &[]string{aliasData},
	})
	assert.NotContains(t, fake.ops[setupOps:], "unmount "+aliasData, "the recorded /data is not detached through the alias")
	_, kept := a.mountedPaths.Load(data[0].Path)
	assert.True(t, kept)
}

func TestPostVolumesMountUndecodableBodyIsA500(t *testing.T) {
	t.Parallel()

	a, _, _ := newVolumesMountTestAPI(t)
	rec := httptest.NewRecorder()
	a.PostVolumesMount(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/volumes/mount", strings.NewReader(`{"volumeMounts": [`)))
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "it may list unsettled paths")
}

func TestPostVolumesMountKeepsAnUnsettledPathWhenATrackedMountIDIsUnknown(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	candidate := volumes(root, "candidate")[0]
	tracked := volumes(root, "tracked")[0]
	require.NoError(t, os.MkdirAll(candidate.Path, 0o755))
	fake.ids[root] = 1
	fake.ids[candidate.Path] = 2
	fake.mountIDErrors[tracked.Path] = 1
	a.mountedPaths.Store(tracked.Path, "lc-1")

	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{
		LifecycleID: new("lc-1"), VolumeMounts: []VolumeMount{candidate}, UnsettledPaths: &[]string{candidate.Path},
	})
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "the tracked path may own the candidate mount")
	assert.NotContains(t, fake.ops, "unmount "+candidate.Path, "an inconclusive ownership check must not detach it")
	_, trackedStillRecorded := a.mountedPaths.Load(tracked.Path)
	assert.True(t, trackedStillRecorded)
}

// hangLstat makes a's lookups of path block until the test ends, as one into a
// volume whose NFS server stopped answering.
func hangLstat(t *testing.T, a *API, path string) {
	t.Helper()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	a.pathProbeTimeout = 20 * time.Millisecond
	a.lstat = func(p string) (fs.FileInfo, error) {
		if p == path {
			<-release
		}

		return os.Lstat(p)
	}
}

func TestPostVolumesMountGivesUpOnAHungPathLookup(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	mounts := volumes(root, "b")
	hangLstat(t, a, mounts[0].Path)

	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{LifecycleID: new("lc-1"), VolumeMounts: mounts})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "nothing was mounted")
	assert.Contains(t, rec.Body.String(), "did not complete")
	assert.Equal(t, mounts[0].Path, rec.Header().Get(failedVolumeHeader))
	assert.Empty(t, fake.mounted)
	assert.True(t, a.initLock.TryAcquire(1), "the lock is not held by the hung lookup")
}

func TestPostVolumesMountHungLookupOfAnUnsettledPathIsA500(t *testing.T) {
	t.Parallel()

	a, fake, root := newVolumesMountTestAPI(t)
	mounts := volumes(root, "b")
	hangLstat(t, a, mounts[0].Path)

	rec := postVolumesMount(t, a, PostVolumesMountJSONBody{
		LifecycleID: new("lc-1"), VolumeMounts: mounts, UnsettledPaths: &[]string{mounts[0].Path},
	})
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "what an earlier call left at the path is still unknown")
	assert.Empty(t, fake.ops)
	assert.True(t, a.initLock.TryAcquire(1), "the lock is not held by the hung lookup")
}

//nolint:paralleltest // checks the package-global pathProbeSlots; must run serially
func TestBoundedProbe(t *testing.T) {
	value, err := boundedProbe(time.Second, func() (int, error) { return 7, nil })
	require.NoError(t, err)
	assert.Equal(t, 7, value)

	release := make(chan struct{})
	closed := false
	defer func() {
		if !closed {
			close(release)
		}
	}()
	_, err = boundedProbe(10*time.Millisecond, func() (int, error) {
		<-release

		return 0, nil
	})
	require.ErrorIs(t, err, errPathProbeTimeout)
	close(release)
	closed = true
	require.Eventually(t, func() bool { return len(pathProbeSlots) == 0 }, time.Second, time.Millisecond)
}

//nolint:paralleltest // fills the package-global pathProbeSlots; must run serially
func TestBoundedProbeCapsWorkersLeftInBlockedSyscalls(t *testing.T) {
	release := make(chan struct{})
	closed := false
	defer func() {
		if !closed {
			close(release)
		}
	}()

	var started atomic.Int32
	const calls = maxConcurrentPathProbes + 4
	start := make(chan struct{})
	results := make(chan error, calls)
	for range calls {
		go func() {
			<-start
			_, err := boundedProbe(50*time.Millisecond, func() (int, error) {
				started.Add(1)
				<-release

				return 0, nil
			})
			results <- err
		}()
	}
	close(start)
	for range calls {
		require.ErrorIs(t, <-results, errPathProbeTimeout)
	}
	assert.Equal(t, int32(maxConcurrentPathProbes), started.Load())

	close(release)
	closed = true
	require.Eventually(t, func() bool { return len(pathProbeSlots) == 0 }, time.Second, time.Millisecond)
}
