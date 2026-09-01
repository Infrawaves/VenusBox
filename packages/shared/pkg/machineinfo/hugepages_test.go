package machineinfo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const testHugePageSize = 2 * 1024 * 1024

func TestHasHugePagesHeadroom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pool HugePagePool
		max  int
		want bool
	}{
		{
			name: "no sample yet is admitted",
			pool: HugePagePool{},
			max:  80,
			want: true,
		},
		{
			name: "no sample yet, even with stale-looking counts",
			pool: HugePagePool{Total: 1000, Used: 999},
			max:  80,
			want: true,
		},
		{
			name: "real sample of a node with no pool",
			pool: HugePagePool{SizeBytes: testHugePageSize},
			max:  80,
			want: true,
		},
		{
			name: "non-positive threshold disables the check",
			pool: HugePagePool{Total: 1000, Used: 999, SizeBytes: testHugePageSize},
			max:  0,
			want: true,
		},
		{
			name: "negative threshold disables the check",
			pool: HugePagePool{Total: 1000, Used: 999, SizeBytes: testHugePageSize},
			max:  -1,
			want: true,
		},
		{
			name: "just under the threshold",
			pool: HugePagePool{Total: 1000, Used: 799, SizeBytes: testHugePageSize},
			max:  80,
			want: true,
		},
		{
			name: "exactly at the threshold is rejected",
			pool: HugePagePool{Total: 1000, Used: 800, SizeBytes: testHugePageSize},
			max:  80,
			want: false,
		},
		{
			name: "fully exhausted",
			pool: HugePagePool{Total: 1000, Used: 1000, SizeBytes: testHugePageSize},
			max:  80,
			want: false,
		},
		{
			name: "used beyond total is rejected",
			pool: HugePagePool{Total: 1000, Used: 1100, SizeBytes: testHugePageSize},
			max:  80,
			want: false,
		},
		{
			name: "reserved pages count toward usage",
			pool: HugePagePool{Total: 1000, Used: 700, Reserved: 150, SizeBytes: testHugePageSize},
			max:  80,
			want: false,
		},
		{
			name: "reserved pages keep a node eligible when the sum fits",
			pool: HugePagePool{Total: 1000, Used: 700, Reserved: 50, SizeBytes: testHugePageSize},
			max:  80,
			want: true,
		},
		{
			name: "threshold of 100 admits everything short of exhaustion",
			pool: HugePagePool{Total: 1000, Used: 999, SizeBytes: testHugePageSize},
			max:  100,
			want: true,
		},
		{
			name: "1536 GiB node at the real-world default",
			pool: HugePagePool{Total: 611840, Used: 14000, SizeBytes: testHugePageSize},
			max:  80,
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, HasHugePagesHeadroom(tt.pool, tt.max))
		})
	}
}

// TestHasHugePagesHeadroomNoOverflow guards the used*100 and Total*max
// multiplications at pool sizes far beyond any real host.
func TestHasHugePagesHeadroomNoOverflow(t *testing.T) {
	t.Parallel()

	const huge = uint64(1) << 40

	assert.True(t, HasHugePagesHeadroom(
		HugePagePool{Total: huge, Used: huge / 100, SizeBytes: testHugePageSize}, 80))
	assert.False(t, HasHugePagesHeadroom(
		HugePagePool{Total: huge, Used: huge / 100 * 90, SizeBytes: testHugePageSize}, 80))
}

func TestHugePagePoolHasSample(t *testing.T) {
	t.Parallel()

	assert.False(t, HugePagePool{}.HasSample())
	assert.False(t, HugePagePool{Total: 100, Used: 50}.HasSample())
	assert.True(t, HugePagePool{SizeBytes: testHugePageSize}.HasSample())
}
