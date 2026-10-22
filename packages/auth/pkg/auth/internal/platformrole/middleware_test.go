package platformrole

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/authcontext"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

func TestResolve(t *testing.T) {
	t.Parallel()

	userID := uuid.New()

	tests := []struct {
		name     string
		setUser  bool
		lookup   Lookup
		wantRole types.PlatformRole
		wantSet  bool
	}{
		{
			name:    "no authenticated user is a no-op",
			setUser: false,
			lookup: func(context.Context, uuid.UUID) (types.PlatformRole, bool, error) {
				return types.PlatformRoleSuperAdmin, true, nil
			},
			wantSet: false,
		},
		{
			name:    "user with a role gets it set",
			setUser: true,
			lookup: func(context.Context, uuid.UUID) (types.PlatformRole, bool, error) {
				return types.PlatformRoleSuperAdmin, true, nil
			},
			wantRole: types.PlatformRoleSuperAdmin,
			wantSet:  true,
		},
		{
			name:    "user without a role is a no-op",
			setUser: true,
			lookup:  func(context.Context, uuid.UUID) (types.PlatformRole, bool, error) { return "", false, nil },
			wantSet: false,
		},
		{
			name:    "lookup error fails closed",
			setUser: true,
			lookup: func(context.Context, uuid.UUID) (types.PlatformRole, bool, error) {
				return "", false, errors.New("db down")
			},
			wantSet: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := gin.New()

			var gotRole types.PlatformRole
			var gotSet bool
			r.GET("/x", func(c *gin.Context) {
				if tc.setUser {
					authcontext.SetUserID(c, userID)
				}
				Resolve(tc.lookup)(c)
				gotRole, gotSet = authcontext.GetPlatformRole(c)
				c.Status(http.StatusOK)
			})

			r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))

			require.Equal(t, tc.wantSet, gotSet)
			if tc.wantSet {
				assert.Equal(t, tc.wantRole, gotRole)
			}
		})
	}
}

func TestRequire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		required   types.PlatformRole
		setRole    *types.PlatformRole
		wantStatus int
		wantRan    bool
	}{
		{
			name:       "no platform role is rejected",
			required:   types.PlatformRoleAdmin,
			setRole:    nil,
			wantStatus: http.StatusForbidden,
			wantRan:    false,
		},
		{
			name:       "platform_admin cannot reach super_admin route",
			required:   types.PlatformRoleSuperAdmin,
			setRole:    new(types.PlatformRoleAdmin),
			wantStatus: http.StatusForbidden,
			wantRan:    false,
		},
		{
			name:       "super_admin reaches super_admin route",
			required:   types.PlatformRoleSuperAdmin,
			setRole:    new(types.PlatformRoleSuperAdmin),
			wantStatus: http.StatusOK,
			wantRan:    true,
		},
		{
			name:       "platform_admin reaches platform_admin route",
			required:   types.PlatformRoleAdmin,
			setRole:    new(types.PlatformRoleAdmin),
			wantStatus: http.StatusOK,
			wantRan:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w, ran := runRequire(t, tc.required, tc.setRole)
			require.Equal(t, tc.wantStatus, w.Code)
			require.Equal(t, tc.wantRan, ran)
		})
	}
}

func runRequire(t *testing.T, required types.PlatformRole, setRole *types.PlatformRole) (*httptest.ResponseRecorder, bool) {
	t.Helper()

	handlerRan := false

	r := gin.New()
	r.GET("/x", func(c *gin.Context) {
		if setRole != nil {
			authcontext.SetPlatformRole(c, *setRole)
		}
		Require(required)(c)
		if !c.IsAborted() {
			handlerRan = true
			c.Status(http.StatusOK)
		}
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil)
	r.ServeHTTP(w, req)

	return w, handlerRan
}
