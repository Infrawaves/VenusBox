package platformrole

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/authcontext"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
)

// Require returns a gin middleware that denies callers whose platform role is below required. Must run after auth.
func Require(required types.PlatformRole) gin.HandlerFunc {
	return func(c *gin.Context) {
		role, ok := authcontext.GetPlatformRole(c)
		if !ok || !role.IsAtLeast(required) {
			apierrors.SendAPIStoreError(c, http.StatusForbidden, "This endpoint requires elevated platform privileges.")
			c.Abort()

			return
		}

		c.Next()
	}
}

// Lookup resolves a user's platform role. ok is false when the user has none; err is only for real failures.
type Lookup func(ctx context.Context, userID uuid.UUID) (role types.PlatformRole, ok bool, err error)

// Resolve returns a gin middleware that records the authenticated user's platform role on the context.
// It is a no-op for callers without a user (e.g. team API key) and fails closed (records nothing) on lookup error. Must run after auth.
func Resolve(lookup Lookup) gin.HandlerFunc {
	return func(c *gin.Context) {
		userID, hasUser := authcontext.GetUserID(c)
		if !hasUser {
			c.Next()

			return
		}

		role, ok, err := lookup(c.Request.Context(), userID)
		if err != nil || !ok {
			c.Next()

			return
		}

		authcontext.SetPlatformRole(c, role)
		c.Next()
	}
}
