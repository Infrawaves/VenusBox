package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

// platformRoleRoutes maps a guarded route to the minimum platform role it requires.
// Global read-only views are platform_admin; high-risk operations should be super_admin.
var platformRoleRoutes = map[string]types.PlatformRole{
	http.MethodGet + " /super-admin/templates": types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/builds":    types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/teams":     types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/users":     types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/stats":     types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/teams/:teamID": types.PlatformRoleAdmin,
	http.MethodGet + " /super-admin/users/:userID": types.PlatformRoleAdmin,

	// Admin management and quota edits are super_admin only (high-risk writes).
	http.MethodPut + " /super-admin/teams/:teamID/limits":           types.PlatformRoleSuperAdmin,
	http.MethodPut + " /super-admin/teams/:teamID/block":            types.PlatformRoleSuperAdmin,
	http.MethodGet + " /super-admin/admins":                         types.PlatformRoleSuperAdmin,
	http.MethodPost + " /super-admin/admins":                        types.PlatformRoleSuperAdmin,
	http.MethodDelete + " /super-admin/admins/:userID":              types.PlatformRoleSuperAdmin,
	http.MethodDelete + " /super-admin/users/:userID":               types.PlatformRoleSuperAdmin,
	http.MethodPost + " /super-admin/users/:userID/teams":           types.PlatformRoleSuperAdmin,
	http.MethodDelete + " /super-admin/users/:userID/teams/:teamID": types.PlatformRoleSuperAdmin,
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
