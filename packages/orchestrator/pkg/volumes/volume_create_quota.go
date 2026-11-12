//go:build linux

package volumes

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

const volumeFilesystemMarker = ".e2b-volume-filesystem"

// ValidateVolumeMount allows the API to verify node storage before it binds a
// team to a filesystem or changes the cloud quota. It creates no directories.
func (s *Service) ValidateVolumeMount(_ context.Context, request *orchestrator.CreateVolumeWithQuotaRequest) (*orchestrator.CreateVolumeResponse, error) {
	mounts, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "reading volume mounts: %v", err)
	}
	defer mounts.Close()

	return s.validateVolumeMount(request, mounts)
}

func (s *Service) validateVolumeMount(request *orchestrator.CreateVolumeWithQuotaRequest, mounts io.Reader) (*orchestrator.CreateVolumeResponse, error) {
	root, err := s.openQuotaRequestRoot(request, mounts)
	if err != nil {
		return nil, err
	}
	defer root.file.Close()
	volumeType := request.GetVolume().GetVolumeType()

	return &orchestrator.CreateVolumeResponse{VolumeType: &volumeType}, nil
}

// CreateVolumeWithQuota verifies that the local volume path is inside the
// fileset the API prepared. The separate RPC makes older nodes fail closed.
func (s *Service) CreateVolumeWithQuota(ctx context.Context, request *orchestrator.CreateVolumeWithQuotaRequest) (*orchestrator.CreateVolumeResponse, error) {
	mounts, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "reading volume mounts: %v", err)
	}
	defer mounts.Close()

	return s.createVolumeWithQuota(ctx, request, mounts)
}

func (s *Service) createVolumeWithQuota(ctx context.Context, request *orchestrator.CreateVolumeWithQuotaRequest, mounts io.Reader) (_ *orchestrator.CreateVolumeResponse, err error) {
	_, span := tracer.Start(ctx, "create volume with quota")
	defer func() {
		setSpanStatus(span, err)
		span.End()
	}()

	root, err := s.openQuotaRequestRoot(request, mounts)
	if err != nil {
		return nil, err
	}
	defer root.file.Close()

	// Keep each creation relative to an already verified directory descriptor.
	// NO_XDEV also rejects a bind mount of another subtree on the same device.
	team, err := makeQuotaDirectory(root.file, root.teamName)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "opening quota team directory: %v", err)
	}
	defer team.Close()
	created, err := makeQuotaDirectory(team, root.volumeName)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "creating quota volume directory: %v", err)
	}
	defer created.Close()

	volumeType := request.GetVolume().GetVolumeType()

	return &orchestrator.CreateVolumeResponse{VolumeType: &volumeType}, nil
}

type quotaVolumeRoot struct {
	file       *os.File
	teamName   string
	volumeName string
}

func (s *Service) openQuotaRequestRoot(request *orchestrator.CreateVolumeWithQuotaRequest, mounts io.Reader) (*quotaVolumeRoot, error) {
	volume := request.GetVolume()
	teamID, err := uuid.Parse(volume.GetTeamId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid team ID")
	}
	volumeID, err := uuid.Parse(volume.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid volume ID")
	}
	if request.GetFileSystemId() == "" || request.GetFilesetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "filesystem ID and fileset path are required")
	}
	rootPath, ok := s.config.PersistentVolumeMounts[volume.GetVolumeType()]
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "quota volume type is not mounted")
	}

	root, err := openQuotaVolumeRoot(rootPath, request.GetFileSystemId(), request.GetFilesetPath(), teamID, mounts)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "verifying quota volume storage: %v", err)
	}
	// The team and volume directories are deliberately not looked up here. In
	// preflight the team's directory usually does not exist yet, and an NFS
	// lookup of it would leave a cached "missing" entry that hides the directory
	// vePFS creates with the fileset moments later. Creation opens both
	// directories with RESOLVE_NO_SYMLINKS|RESOLVE_NO_XDEV, which rejects
	// symlinks and submounts there instead.
	return &quotaVolumeRoot{file: root, teamName: "team-" + teamID.String(), volumeName: "vol-" + volumeID.String()}, nil
}

func openQuotaVolumeRoot(rootPath, filesystemID, filesetPath string, teamID uuid.UUID, mounts io.Reader) (*os.File, error) {
	if !filepath.IsAbs(rootPath) {
		return nil, errors.New("volume mount path must be absolute")
	}
	rootPath = filepath.Clean(rootPath)
	rootFD, err := unix.Openat2(unix.AT_FDCWD, rootPath, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return nil, fmt.Errorf("opening volume root: %w", err)
	}
	root := os.NewFile(uintptr(rootFD), rootPath)
	succeeded := false
	defer func() {
		if !succeeded {
			root.Close()
		}
	}()
	mountID, err := quotaMountID(root)
	if err != nil {
		return nil, err
	}
	mount, err := findQuotaMount(mounts, mountID)
	if err != nil {
		return nil, err
	}
	if mount.root != "/" {
		return nil, errors.New("volume storage is a mounted filesystem subtree")
	}
	// The supported vePFS transport is NFS. A local directory left behind by a
	// failed or missing mount must never pass merely because it has a marker.
	if mount.fsType != "nfs" && mount.fsType != "nfs4" {
		return nil, fmt.Errorf("volume storage is not an NFS filesystem: %s", mount.fsType)
	}
	relative, err := filepath.Rel(mount.mountpoint, rootPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, "../") {
		return nil, errors.New("volume root is outside its filesystem mount")
	}
	expectedPath := filepath.Join("/", relative, "team-"+teamID.String()) + "/"
	if filesetPath != expectedPath {
		return nil, fmt.Errorf("fileset path %q does not match local volume path %q", filesetPath, expectedPath)
	}

	// NFS export names (such as :/vepfs) need not be '/'. The operator places
	// this marker at the actual cloud filesystem root, attesting the export's
	// identity and root. Never create or infer it from the API request.
	mountFD, err := unix.Openat2(unix.AT_FDCWD, mount.mountpoint, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return nil, fmt.Errorf("opening filesystem mount: %w", err)
	}
	mountRoot := os.NewFile(uintptr(mountFD), mount.mountpoint)
	defer mountRoot.Close()
	openedMountID, err := quotaMountID(mountRoot)
	if err != nil {
		return nil, err
	}
	if openedMountID != mountID {
		return nil, errors.New("filesystem mount changed during verification")
	}
	if err := verifyQuotaMarker(mountRoot, filesystemID); err != nil {
		return nil, err
	}
	succeeded = true

	return root, nil
}

func quotaMountID(file *os.File) (uint64, error) {
	var stat unix.Statx_t
	if err := unix.Statx(int(file.Fd()), "", unix.AT_EMPTY_PATH, unix.STATX_MNT_ID, &stat); err != nil {
		return 0, fmt.Errorf("reading filesystem mount identity: %w", err)
	}
	if stat.Mask&unix.STATX_MNT_ID == 0 {
		return 0, errors.New("kernel does not report filesystem mount identity")
	}

	return stat.Mnt_id, nil
}

func verifyQuotaMarker(root *os.File, filesystemID string) error {
	fd, err := unix.Openat2(int(root.Fd()), volumeFilesystemMarker, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_NONBLOCK | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV,
	})
	if err != nil {
		return fmt.Errorf("opening filesystem identity marker: %w", err)
	}
	marker := os.NewFile(uintptr(fd), volumeFilesystemMarker)
	defer marker.Close()
	info, err := marker.Stat()
	if err != nil {
		return fmt.Errorf("reading filesystem marker metadata: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("filesystem identity marker must be a regular file")
	}
	content, err := io.ReadAll(io.LimitReader(marker, 257))
	if err != nil {
		return fmt.Errorf("reading filesystem identity marker: %w", err)
	}
	if len(content) > 256 || strings.TrimSuffix(string(content), "\n") != filesystemID {
		return errors.New("filesystem identity marker does not match the quota filesystem")
	}

	return nil
}

func makeQuotaDirectory(parent *os.File, name string) (*os.File, error) {
	mkdirErr := unix.Mkdirat(int(parent.Fd()), name, 0o700)
	if mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
		return nil, mkdirErr
	}
	dir, err := openQuotaDirectory(parent, name)
	if mkdirErr != nil && errors.Is(err, unix.ENOENT) {
		// The server has the directory but this node's NFS client still
		// caches an earlier lookup that found nothing. Clearing it in-process
		// is not reliable; it expires within about a minute.
		return nil, fmt.Errorf("%q exists on the server but this node's NFS client caches it as missing; "+
			"retry in a minute, and avoid looking the path up before it exists (or mount with lookupcache=positive): %w", name, err)
	}

	return dir, err
}

func openQuotaDirectory(parent *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat2(int(parent.Fd()), name, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV,
	})
	if err != nil {
		return nil, err
	}

	return os.NewFile(uintptr(fd), name), nil
}

type quotaMount struct {
	root       string
	mountpoint string
	fsType     string
}

func findQuotaMount(reader io.Reader, mountID uint64) (quotaMount, error) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		before, after, ok := strings.Cut(scanner.Text(), " - ")
		fields, filesystem := strings.Fields(before), strings.Fields(after)
		if !ok || len(fields) < 6 || len(filesystem) < 3 {
			return quotaMount{}, errors.New("invalid filesystem mount information")
		}
		id, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return quotaMount{}, fmt.Errorf("invalid filesystem mount identity: %w", err)
		}
		if id == mountID {
			unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, "\\")

			return quotaMount{root: unescape.Replace(fields[3]), mountpoint: unescape.Replace(fields[4]), fsType: filesystem[0]}, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return quotaMount{}, fmt.Errorf("reading filesystem mounts: %w", err)
	}

	return quotaMount{}, errors.New("volume filesystem mount was not found")
}
