package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

func TestEnforcePlatformRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		setRole    *types.PlatformRole
		wantStatus int
		wantRan    bool
	}{
		{
			name:       "guarded route without role is rejected",
			path:       "/super-admin/sandboxes",
			setRole:    nil,
			wantStatus: http.StatusForbidden,
			wantRan:    false,
		},
		{
			name:       "guarded route allows platform_admin",
			path:       "/super-admin/sandboxes",
			setRole:    new(types.PlatformRoleAdmin),
			wantStatus: http.StatusOK,
			wantRan:    true,
		},
		{
			name:       "guarded route allows super_admin",
			path:       "/super-admin/sandboxes",
			setRole:    new(types.PlatformRoleSuperAdmin),
			wantStatus: http.StatusOK,
			wantRan:    true,
		},
		{
			name:       "unguarded route passes without any role",
			path:       "/sandboxes",
			setRole:    nil,
			wantStatus: http.StatusOK,
			wantRan:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := gin.New()
			handlerRan := false
			r.GET(tc.path, func(c *gin.Context) {
				if tc.setRole != nil {
					auth.SetPlatformRoleForTest(t, c, *tc.setRole)
				}
				EnforcePlatformRole()(c)
				if !c.IsAborted() {
					handlerRan = true
					c.Status(http.StatusOK)
				}
			})

			w := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tc.path, nil)
			r.ServeHTTP(w, req)

			require.Equal(t, tc.wantStatus, w.Code)
			require.Equal(t, tc.wantRan, handlerRan)
		})
	}
}
