package auth

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/authcontext"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/platformrole"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

func GetUserID(c *gin.Context) (uuid.UUID, bool) {
	return authcontext.GetUserID(c)
}

func MustGetUserID(c *gin.Context) uuid.UUID {
	return authcontext.MustGetUserID(c)
}

func MustGetTeamInfo(c *gin.Context) *types.Team {
	return authcontext.MustGetTeamInfo(c)
}

func MustGetTeamID(c *gin.Context) uuid.UUID {
	return authcontext.MustGetTeamID(c)
}

func GetTeamInfo(c *gin.Context) (*types.Team, bool) {
	return authcontext.GetTeamInfo(c)
}

func GetServiceIssuer(c *gin.Context) (string, bool) {
	return authcontext.GetServiceIssuer(c)
}

// SetPlatformRole records the caller's platform role on the gin context.
func SetPlatformRole(c *gin.Context, role types.PlatformRole) {
	authcontext.SetPlatformRole(c, role)
}

// GetPlatformRole returns the caller's platform role; false when the caller has none.
func GetPlatformRole(c *gin.Context) (types.PlatformRole, bool) {
	return authcontext.GetPlatformRole(c)
}

// RequirePlatformRole returns a gin middleware that denies callers below the required platform role with 403.
func RequirePlatformRole(required types.PlatformRole) gin.HandlerFunc {
	return platformrole.Require(required)
}

// PlatformRoleLookup resolves a user's platform role; ok is false when the user has none.
type PlatformRoleLookup = platformrole.Lookup

// ResolvePlatformRole returns a gin middleware that records the authenticated user's platform role on the context.
func ResolvePlatformRole(lookup PlatformRoleLookup) gin.HandlerFunc {
	return platformrole.Resolve(lookup)
}
