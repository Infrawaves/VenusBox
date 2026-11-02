package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

// platformRoleRoutes maps a guarded route to the minimum platform role it requires.
// Global read-only views are platform_admin. /super-admin/me is intentionally absent:
// it reports the caller's role and must not 403, so the dashboard can gate its UI.
var platformRoleRoutes = map[string]types.PlatformRole{
	http.MethodGet + " /super-admin/teams":         types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/users":         types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/users/:userID": types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/templates":     types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/builds":        types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/snapshots":     types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/stats":         types.PlatformRoleAdmin,

	// High-risk writes require super_admin.
	http.MethodPut + " /super-admin/teams/:teamID/limits": types.PlatformRoleSuperAdmin,
	http.MethodPut + " /super-admin/teams/:teamID/block":  types.PlatformRoleSuperAdmin,
}

// EnforcePlatformRole gates the routes in platformRoleRoutes on their minimum platform role. Must run after auth.
func EnforcePlatformRole() gin.HandlerFunc {
	guards := make(map[string]gin.HandlerFunc, len(platformRoleRoutes))
	for route, role := range platformRoleRoutes {
		guards[route] = auth.RequirePlatformRole(role)
	}

	return func(c *gin.Context) {
		if guard, ok := guards[c.Request.Method+" "+c.FullPath()]; ok {
			guard(c)

			return
		}

		c.Next()
	}
}
