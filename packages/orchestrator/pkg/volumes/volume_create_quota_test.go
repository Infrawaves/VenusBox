//go:build linux

package volumes

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

func quotaTestMountInfo(t *testing.T, root, mountpoint, mountRoot, fsType string) string {
	t.Helper()
	file, err := os.Open(root)
	require.NoError(t, err)
	defer file.Close()
	id, err := quotaMountID(file)
	require.NoError(t, err)
	escape := strings.NewReplacer(`\`, `\134`, " ", `\040`, "\t", `\011`, "\n", `\012`)

	return fmt.Sprintf("%d 1 0:42 %s %s rw - %s export.example.vepfs.ivolces.com:/vepfs rw\n",
		id, escape.Replace(mountRoot), escape.Replace(mountpoint), fsType)
}

func TestCreateVolumeWithQuotaVerifiesStorage(t *testing.T) {
	t.Parallel()
	const filesystemID = "vepfs-test-filesystem"
	for _, test := range []struct {
		name        string
		volumeRoot  string
		fsType      string
		mountRoot   string
		marker      string
		filesetPath string
		wantError   bool
	}{
		{name: "filesystem root", fsType: "nfs", mountRoot: "/", marker: filesystemID},
		{name: "actual volumes subdirectory", volumeRoot: "volumes", fsType: "nfs", mountRoot: "/", marker: filesystemID + "\n"},
		{name: "NFS4 and escaped mountpoint", volumeRoot: "volumes", fsType: "nfs4", mountRoot: "/", marker: filesystemID},
		{name: "wrong filesystem", volumeRoot: "volumes", fsType: "nfs", mountRoot: "/", marker: "vepfs-other", wantError: true},
		{name: "missing marker", volumeRoot: "volumes", fsType: "nfs", mountRoot: "/", wantError: true},
		{name: "local fallback directory", volumeRoot: "volumes", fsType: "ext4", mountRoot: "/", marker: filesystemID, wantError: true},
		{name: "bind mounted subtree", fsType: "nfs", mountRoot: "/volumes", marker: filesystemID, wantError: true},
		{name: "wrong fileset parent", volumeRoot: "volumes", fsType: "nfs", mountRoot: "/", marker: filesystemID, filesetPath: "/team-%s/", wantError: true},
		{name: "fileset path traversal", volumeRoot: "volumes", fsType: "nfs", mountRoot: "/", marker: filesystemID, filesetPath: "/volumes/../team-%s/", wantError: true},
		{name: "different team fileset", volumeRoot: "volumes", fsType: "nfs", mountRoot: "/", marker: filesystemID, filesetPath: "/volumes/team-" + uuid.NewString() + "/%s", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mountpoint := filepath.Join(t.TempDir(), "vepfs mount")
			root := filepath.Join(mountpoint, test.volumeRoot)
			require.NoError(t, os.MkdirAll(root, 0o700))
			if test.marker != "" {
				require.NoError(t, os.WriteFile(filepath.Join(mountpoint, volumeFilesystemMarker), []byte(test.marker), 0o600))
			}
			teamID := uuid.New()
			volumeID := uuid.New()
			filesetPath := filepath.Join("/", test.volumeRoot, "team-"+teamID.String()) + "/"
			if test.filesetPath != "" {
				filesetPath = fmt.Sprintf(test.filesetPath, teamID)
			}
			config := cfg.Config{PersistentVolumeMounts: map[string]string{volumeType: root}}
			service := New(config, chrooted.NewBuilder(config))
			request := &orchestrator.CreateVolumeWithQuotaRequest{
				Volume:       &orchestrator.VolumeInfo{TeamId: teamID.String(), VolumeId: volumeID.String(), VolumeType: volumeType},
				FileSystemId: filesystemID,
				FilesetPath:  filesetPath,
			}
			mounts := quotaTestMountInfo(t, root, mountpoint, test.mountRoot, test.fsType)
			createdPath := filepath.Join(root, "team-"+teamID.String(), "vol-"+volumeID.String())
			_, preflightErr := service.validateVolumeMount(request, strings.NewReader(mounts))
			require.NoDirExists(t, filepath.Dir(createdPath), "preflight must not create the team directory")
			if test.wantError {
				require.Equal(t, codes.FailedPrecondition, status.Code(preflightErr), "%v", preflightErr)
			} else {
				require.NoError(t, preflightErr)
			}
			response, err := service.createVolumeWithQuota(t.Context(), request, strings.NewReader(mounts))
			if test.wantError {
				require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
				require.NoDirExists(t, createdPath)

				return
			}
			require.NoError(t, err)
			require.Equal(t, volumeType, response.GetVolumeType())
			require.DirExists(t, createdPath)
			// A retried call returns the same directory and still verifies it.
			_, err = service.createVolumeWithQuota(t.Context(), request, strings.NewReader(mounts))
			require.NoError(t, err)
		})
	}
}

func TestCreateVolumeWithQuotaRejectsSymlinks(t *testing.T) {
	t.Parallel()
	for _, target := range []string{"marker", "volume root", "team", "volume"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			mountpoint := t.TempDir()
			root := filepath.Join(mountpoint, "volumes")
			require.NoError(t, os.Mkdir(root, 0o700))
			teamID, volumeID := uuid.New(), uuid.New()
			teamPath := filepath.Join(root, "team-"+teamID.String())
			volumePath := filepath.Join(teamPath, "vol-"+volumeID.String())
			markerPath := filepath.Join(mountpoint, volumeFilesystemMarker)
			require.NoError(t, os.WriteFile(markerPath, []byte("vepfs-test"), 0o600))
			outside := t.TempDir()
			switch target {
			case "marker":
				require.NoError(t, os.Remove(markerPath))
				require.NoError(t, os.WriteFile(filepath.Join(outside, "marker"), []byte("vepfs-test"), 0o600))
				require.NoError(t, os.Symlink(filepath.Join(outside, "marker"), markerPath))
			case "volume root":
				require.NoError(t, os.Remove(root))
				require.NoError(t, os.Symlink(outside, root))
			case "team":
				require.NoError(t, os.Symlink(outside, teamPath))
			case "volume":
				require.NoError(t, os.Mkdir(teamPath, 0o700))
				require.NoError(t, os.Symlink(outside, volumePath))
			}
			config := cfg.Config{PersistentVolumeMounts: map[string]string{volumeType: root}}
			service := New(config, chrooted.NewBuilder(config))
			request := &orchestrator.CreateVolumeWithQuotaRequest{
				Volume:       &orchestrator.VolumeInfo{TeamId: teamID.String(), VolumeId: volumeID.String(), VolumeType: volumeType},
				FileSystemId: "vepfs-test",
				FilesetPath:  "/volumes/team-" + teamID.String() + "/",
			}
			mounts := quotaTestMountInfo(t, root, mountpoint, "/", "nfs")
			_, err := service.validateVolumeMount(request, strings.NewReader(mounts))
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
			_, err = service.createVolumeWithQuota(t.Context(), request, strings.NewReader(mounts))
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
			entries, err := os.ReadDir(outside)
			require.NoError(t, err)
			if target != "marker" {
				require.Empty(t, entries, "failed verification must not create a directory outside the fileset")
			}
		})
	}
}

func TestQuotaMarkerRejectsNonRegularFile(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	require.NoError(t, unix.Mkfifo(filepath.Join(path, volumeFilesystemMarker), 0o600))
	root, err := os.Open(path)
	require.NoError(t, err)
	defer root.Close()
	require.ErrorContains(t, verifyQuotaMarker(root, "vepfs-test"), "regular file")
}

func TestQuotaDirectoryRejectsSubmount(t *testing.T) {
	t.Parallel()
	root, err := os.Open("/")
	require.NoError(t, err)
	defer root.Close()
	proc, err := os.Open("/proc")
	require.NoError(t, err)
	defer proc.Close()
	rootID, err := quotaMountID(root)
	require.NoError(t, err)
	procID, err := quotaMountID(proc)
	require.NoError(t, err)
	if rootID == procID {
		t.Skip("/proc is not a separate mount")
	}
	_, err = makeQuotaDirectory(t.Context(), root, "proc")
	require.ErrorIs(t, err, unix.EXDEV)
}

// fakeQuotaDirectoryOps simulates an NFS client whose cached negative lookup
// hides a directory the server already has.
type fakeQuotaDirectoryOps struct {
	mkdirErr     error
	staleOpens   int
	opens        int
	refreshes    int
	directory    *os.File
	refreshError error
}

func (f *fakeQuotaDirectoryOps) ops(timeout time.Duration) quotaDirectoryOps {
	return quotaDirectoryOps{
		mkdirat: func(*os.File, string) error { return f.mkdirErr },
		open: func(*os.File, string) (*os.File, error) {
			f.opens++
			if f.opens <= f.staleOpens {
				return nil, unix.ENOENT
			}

			return f.directory, nil
		},
		refresh: func(*os.File) error {
			f.refreshes++

			return f.refreshError
		},
		timeout:  timeout,
		interval: time.Millisecond,
	}
}

func openTestDirectory(t *testing.T) *os.File {
	t.Helper()
	dir, err := os.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { dir.Close() })

	return dir
}

func TestMakeQuotaDirectoryRecoversFromStaleNegativeLookup(t *testing.T) {
	t.Parallel()
	parent := openTestDirectory(t)
	fake := &fakeQuotaDirectoryOps{mkdirErr: unix.EEXIST, staleOpens: 2, directory: openTestDirectory(t)}

	dir, err := fake.ops(time.Minute).makeDirectory(t.Context(), parent, "team-x")
	require.NoError(t, err)
	require.Same(t, fake.directory, dir)
	require.Equal(t, 3, fake.opens)
	require.Equal(t, 2, fake.refreshes, "each stale lookup must refresh the parent before retrying")
}

func TestMakeQuotaDirectoryDoesNotRetryAfterOwnMkdir(t *testing.T) {
	t.Parallel()
	// mkdir succeeded here, so ENOENT means the directory vanished: not stale.
	fake := &fakeQuotaDirectoryOps{staleOpens: 1}

	_, err := fake.ops(time.Minute).makeDirectory(t.Context(), openTestDirectory(t), "team-x")
	require.ErrorIs(t, err, unix.ENOENT)
	require.Equal(t, 1, fake.opens)
	require.Zero(t, fake.refreshes)
}

func TestMakeQuotaDirectoryGivesUpOnPersistentStaleLookup(t *testing.T) {
	t.Parallel()
	fake := &fakeQuotaDirectoryOps{mkdirErr: unix.EEXIST, staleOpens: math.MaxInt}

	_, err := fake.ops(20*time.Millisecond).makeDirectory(t.Context(), openTestDirectory(t), "team-x")
	require.ErrorIs(t, err, unix.ENOENT)
	require.ErrorContains(t, err, "not visible on this node")
	require.Positive(t, fake.refreshes)
}

func TestMakeQuotaDirectoryStopsOnCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	fake := &fakeQuotaDirectoryOps{mkdirErr: unix.EEXIST, staleOpens: math.MaxInt}

	_, err := fake.ops(time.Minute).makeDirectory(ctx, openTestDirectory(t), "team-x")
	require.ErrorIs(t, err, context.Canceled)
}

func TestMakeQuotaDirectoryReturnsFinalErrors(t *testing.T) {
	t.Parallel()
	t.Run("mkdir", func(t *testing.T) {
		t.Parallel()
		fake := &fakeQuotaDirectoryOps{mkdirErr: unix.EACCES}
		_, err := fake.ops(time.Minute).makeDirectory(t.Context(), openTestDirectory(t), "team-x")
		require.ErrorIs(t, err, unix.EACCES)
		require.Zero(t, fake.opens)
	})
	t.Run("refresh", func(t *testing.T) {
		t.Parallel()
		fake := &fakeQuotaDirectoryOps{mkdirErr: unix.EEXIST, staleOpens: 1, refreshError: unix.EIO}
		_, err := fake.ops(time.Minute).makeDirectory(t.Context(), openTestDirectory(t), "team-x")
		require.ErrorIs(t, err, unix.EIO)
	})
}

func TestRefreshQuotaDirectory(t *testing.T) {
	t.Parallel()
	dir, err := os.OpenFile(t.TempDir(), unix.O_PATH|unix.O_DIRECTORY, 0)
	require.NoError(t, err)
	defer dir.Close()
	require.NoError(t, refreshQuotaDirectory(dir))
}

func TestQuotaVolumeRejectsMissingMount(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	_, err := openQuotaVolumeRoot(root, "vepfs-test", "/team-"+uuid.NewString()+"/", uuid.New(), strings.NewReader(""))
	require.ErrorContains(t, err, "mount was not found")
}
