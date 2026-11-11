package cfg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVolumeQuotaPaths(t *testing.T) {
	t.Parallel()
	for _, parent := range []string{"/", "/volumes", "/volumes/", "/tenant/volumes"} {
		t.Run(parent, func(t *testing.T) {
			t.Parallel()
			config := VolumeQuotaConfig{
				FileSystems: map[string]string{"vepfs": "vepfs-test"},
				Paths:       map[string]string{"vepfs": parent},
				Region:      "test", AccessKeyID: "test", SecretAccessKey: "test",
			}
			require.NoError(t, config.validate())
		})
	}
	for _, parent := range []string{"", "volumes", "/volumes/../other", "/volumes//other", "//volumes"} {
		t.Run("invalid "+parent, func(t *testing.T) {
			t.Parallel()
			config := VolumeQuotaConfig{
				FileSystems: map[string]string{"vepfs": "vepfs-test"},
				Paths:       map[string]string{"vepfs": parent},
				Region:      "test", AccessKeyID: "test", SecretAccessKey: "test",
			}
			require.Error(t, config.validate())
		})
	}
	require.Error(t, (VolumeQuotaConfig{Paths: map[string]string{"vepfs": "/volumes"}}).validate())
	require.Error(t, (VolumeQuotaConfig{
		FileSystems: map[string]string{"vepfs": "vepfs-test"},
		Paths:       map[string]string{"other": "/volumes"},
		Region:      "test", AccessKeyID: "test", SecretAccessKey: "test",
	}).validate())
}
