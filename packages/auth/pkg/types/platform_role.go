package types

// PlatformRole is the platform-wide privilege level of a caller, orthogonal to team membership.
type PlatformRole string

const (
	// PlatformRoleAdmin can reach cross-team operational read views.
	PlatformRoleAdmin PlatformRole = "platform_admin"

	// PlatformRoleSuperAdmin is the elevated level required by high-risk platform-wide operations.
	PlatformRoleSuperAdmin PlatformRole = "super_admin"
)

// IsAtLeast reports whether the role is at or above the required role (super_admin > platform_admin).
func (r PlatformRole) IsAtLeast(required PlatformRole) bool {
	return platformRoleRank(r) >= platformRoleRank(required)
}

func platformRoleRank(r PlatformRole) int {
	switch r {
	case PlatformRoleSuperAdmin:
		return 2
	case PlatformRoleAdmin:
		return 1
	default:
		return 0
	}
}
