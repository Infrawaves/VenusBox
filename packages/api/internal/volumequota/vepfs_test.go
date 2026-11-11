package volumequota

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/volcengine/volcengine-go-sdk/service/vepfs"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/request"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
)

const (
	testVolumeType   = "vepfs"
	testFileSystemID = "vepfs-test"
)

// fakeVePFS keeps filesets in memory. A created fileset reports Creating on
// the first describe and Running afterwards, as vePFS does.
type fakeVePFS struct {
	mu sync.Mutex

	filesets  map[string]*vepfs.FilesetForDescribeFilesetsOutput // by path
	describes map[string]int                                     // by path, after creation

	createErr error
	quotaErr  error

	creates   int
	quotaSets []int32
}

func newFake() *fakeVePFS {
	return &fakeVePFS{
		filesets:  map[string]*vepfs.FilesetForDescribeFilesetsOutput{},
		describes: map[string]int{},
	}
}

func (f *fakeVePFS) DescribeFilesetsWithContext(_ volcengine.Context, in *vepfs.DescribeFilesetsInput, _ ...request.Option) (*vepfs.DescribeFilesetsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	path := volcengine.StringValue(in.Filters[0].Value)
	fileset, ok := f.filesets[path]
	if !ok {
		return &vepfs.DescribeFilesetsOutput{}, nil
	}

	f.describes[path]++
	if f.describes[path] > 1 && volcengine.StringValue(fileset.Status) == vepfs.EnumOfStatusForDescribeFilesetsOutputCreating {
		fileset.Status = volcengine.String(vepfs.EnumOfStatusForDescribeFilesetsOutputRunning)
	}

	copied := *fileset

	return &vepfs.DescribeFilesetsOutput{Filesets: []*vepfs.FilesetForDescribeFilesetsOutput{&copied}}, nil
}

func (f *fakeVePFS) CreateFilesetWithContext(_ volcengine.Context, in *vepfs.CreateFilesetInput, _ ...request.Option) (*vepfs.CreateFilesetOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.creates++
	if f.createErr != nil {
		return nil, f.createErr
	}

	path := volcengine.StringValue(in.FilesetPath)
	if _, ok := f.filesets[path]; ok {
		return nil, errors.New("fileset already exists")
	}

	id := "fs-" + uuid.NewString()
	f.filesets[path] = &vepfs.FilesetForDescribeFilesetsOutput{
		FilesetId:    volcengine.String(id),
		FilesetName:  in.FilesetName,
		FilesetPath:  in.FilesetPath,
		FileSystemId: in.FileSystemId,
		Status:       volcengine.String(vepfs.EnumOfStatusForDescribeFilesetsOutputCreating),
	}

	return &vepfs.CreateFilesetOutput{FilesetId: volcengine.String(id)}, nil
}

func (f *fakeVePFS) SetFilesetQuotaWithContext(_ volcengine.Context, in *vepfs.SetFilesetQuotaInput, _ ...request.Option) (*vepfs.SetFilesetQuotaOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.quotaErr != nil {
		return nil, f.quotaErr
	}

	for _, fileset := range f.filesets {
		if volcengine.StringValue(fileset.FilesetId) == volcengine.StringValue(in.FilesetId) {
			fileset.CapacityLimit = in.CapacityLimit
			f.quotaSets = append(f.quotaSets, volcengine.Int32Value(in.CapacityLimit))

			return &vepfs.SetFilesetQuotaOutput{}, nil
		}
	}

	return nil, errors.New("fileset not found")
}

func (f *fakeVePFS) limitAt(path string) int32 {
	f.mu.Lock()
	defer f.mu.Unlock()

	if fileset, ok := f.filesets[path]; ok {
		return volcengine.Int32Value(fileset.CapacityLimit)
	}

	return -1
}

func testManager(api filesetAPI) *Manager {
	m := newManager(api, map[string]string{testVolumeType: testFileSystemID})
	m.settleInterval = time.Millisecond
	m.settleTimeout = time.Second

	return m
}

func TestEnsureTeamQuotaCreatesFilesetAndSetsQuota(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := testManager(fake)
	teamID := uuid.New()

	require.NoError(t, m.ensureFileset(t.Context(), testVolumeType, teamID, 100))

	assert.Equal(t, 1, fake.creates)
	path := "/team-" + teamID.String() + "/"
	assert.Equal(t, path, TeamPath(teamID), "vePFS requires a leading and trailing slash")
	assert.Equal(t, int32(100), fake.limitAt(path))
	assert.Equal(t, "team-"+teamID.String(), volcengine.StringValue(fake.filesets[path].FilesetName))
}

// A fileset made in the console may be reported without the trailing slash;
// it is still the team's, so it is adopted rather than created again.
func TestEnsureTeamQuotaAdoptsFilesetWithoutTrailingSlash(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := testManager(fake)
	teamID := uuid.New()
	bare := "/team-" + teamID.String()
	fake.filesets[bare] = &vepfs.FilesetForDescribeFilesetsOutput{
		FilesetId:   volcengine.String("fs-console"),
		FilesetPath: volcengine.String(bare),
		Status:      volcengine.String(vepfs.EnumOfStatusForDescribeFilesetsOutputRunning),
	}

	require.NoError(t, m.ensureFileset(t.Context(), testVolumeType, teamID, 10))
	assert.Zero(t, fake.creates, "the console fileset is adopted")
	assert.Equal(t, int32(10), fake.limitAt(bare))
}

func TestEnsureTeamQuotaIsIdempotentAndFollowsLimitChanges(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := testManager(fake)
	teamID := uuid.New()

	require.NoError(t, m.ensureFileset(t.Context(), testVolumeType, teamID, 100))
	require.NoError(t, m.ensureFileset(t.Context(), testVolumeType, teamID, 100))
	assert.Equal(t, 1, fake.creates, "an existing fileset is reused")
	assert.Equal(t, []int32{100}, fake.quotaSets, "an unchanged limit is not set again")

	require.NoError(t, m.ensureFileset(t.Context(), testVolumeType, teamID, 250))
	assert.Equal(t, 1, fake.creates)
	assert.Equal(t, int32(250), fake.limitAt(TeamPath(teamID)))
}

func TestEnsureTeamQuotaSkipsUnboundedAndUnmanaged(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := testManager(fake)

	require.NoError(t, m.ensureFileset(t.Context(), testVolumeType, uuid.New(), 0))
	require.NoError(t, m.ensureFileset(t.Context(), "nfs", uuid.New(), 100))
	assert.Zero(t, fake.creates)
	assert.False(t, m.Manages("nfs"))
	assert.True(t, m.Manages(testVolumeType))

	var nilManager *Manager
	assert.False(t, nilManager.Manages(testVolumeType))
	require.NoError(t, nilManager.ensureFileset(t.Context(), testVolumeType, uuid.New(), 100))
}

func TestEnsureTeamQuotaRejectsTooLargeQuota(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := testManager(fake)

	err := m.ensureFileset(t.Context(), testVolumeType, uuid.New(), math.MaxInt32+1)
	require.ErrorIs(t, err, ErrQuotaTooLarge)
	assert.Zero(t, fake.creates)
}

func TestEnsureTeamQuotaAdoptsFilesetCreatedConcurrently(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := testManager(fake)
	teamID := uuid.New()
	path := TeamPath(teamID)

	// Another replica created it between this one's lookup and create.
	fake.filesets[path] = &vepfs.FilesetForDescribeFilesetsOutput{
		FilesetId:   volcengine.String("fs-other"),
		FilesetPath: volcengine.String(path),
		Status:      volcengine.String(vepfs.EnumOfStatusForDescribeFilesetsOutputRunning),
	}
	id, err := m.createFileset(t.Context(), testFileSystemID, teamID, path)
	require.NoError(t, err)
	assert.Equal(t, "fs-other", id)
}

func TestEnsureTeamQuotaReportsFailures(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		t.Parallel()

		fake := newFake()
		fake.createErr = errors.New("boom")
		err := testManager(fake).ensureFileset(t.Context(), testVolumeType, uuid.New(), 10)
		require.ErrorContains(t, err, "boom")
	})

	t.Run("quota", func(t *testing.T) {
		t.Parallel()

		fake := newFake()
		fake.quotaErr = errors.New("denied")
		err := testManager(fake).ensureFileset(t.Context(), testVolumeType, uuid.New(), 10)
		require.ErrorContains(t, err, "denied")
	})

	t.Run("error state", func(t *testing.T) {
		t.Parallel()

		fake := newFake()
		teamID := uuid.New()
		path := TeamPath(teamID)
		fake.filesets[path] = &vepfs.FilesetForDescribeFilesetsOutput{
			FilesetId:   volcengine.String("fs-broken"),
			FilesetPath: volcengine.String(path),
			Status:      volcengine.String(vepfs.EnumOfStatusForDescribeFilesetsOutputCreateError),
		}
		err := testManager(fake).ensureFileset(t.Context(), testVolumeType, teamID, 10)
		require.ErrorContains(t, err, "CreateError")
	})
}

func TestNewWithoutFileSystemsIsNil(t *testing.T) {
	t.Parallel()

	m, err := New(cfg.VolumeQuotaConfig{}, nil)
	require.NoError(t, err)
	assert.Nil(t, m)
}

func TestFilesetUsesFilesystemRelativeVolumeRoot(t *testing.T) {
	t.Parallel()
	fake := newFake()
	m := testManager(fake)
	m.paths = map[string]string{testVolumeType: "/volumes/"}
	teamID := uuid.New()
	fsID, filesetPath := m.Fileset(testVolumeType, teamID)
	require.Equal(t, testFileSystemID, fsID)
	require.Equal(t, "/volumes/team-"+teamID.String()+"/", filesetPath)

	// An earlier release may have created an unrelated fileset at /team-ID.
	// Correcting the parent path must neither adopt it nor reuse its name.
	fake.filesets[TeamPath(teamID)] = &vepfs.FilesetForDescribeFilesetsOutput{
		FilesetId:     new("fs-old"),
		FilesetName:   new("team-" + teamID.String()),
		FilesetPath:   new(TeamPath(teamID)),
		Status:        volcengine.String(vepfs.EnumOfStatusForDescribeFilesetsOutputRunning),
		CapacityLimit: volcengine.Int32(5),
	}
	require.NoError(t, m.ensureFileset(t.Context(), testVolumeType, teamID, 10))
	require.Equal(t, int32(10), fake.limitAt(filesetPath))
	require.Equal(t, int32(5), fake.limitAt(TeamPath(teamID)))
	require.NotEqual(t, "team-"+teamID.String(), volcengine.StringValue(fake.filesets[filesetPath].FilesetName))
}
