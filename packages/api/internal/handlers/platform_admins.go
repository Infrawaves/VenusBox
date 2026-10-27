package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// PlatformRoleLookup resolves a signed-in user's platform role from platform_admins, mapping a missing row to ok=false.
func (a *APIStore) PlatformRoleLookup() sharedauth.PlatformRoleLookup {
	return func(ctx context.Context, userID uuid.UUID) (authtypes.PlatformRole, bool, error) {
		role, err := a.sqlcDB.GetPlatformRoleByUserID(ctx, userID)
		if err != nil {
			if dberrors.IsNotFoundError(err) {
				return "", false, nil
			}

			return "", false, err
		}

		return authtypes.PlatformRole(role), true, nil
	}
}

// SeedSuperAdmins upserts each configured user UUID as super_admin. Invalid UUIDs fail fast rather than silently granting nothing.
func (a *APIStore) SeedSuperAdmins(ctx context.Context, userIDs []string) error {
	for _, raw := range userIDs {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}

		userID, err := uuid.Parse(trimmed)
		if err != nil {
			return fmt.Errorf("invalid SUPER_ADMIN_USER_IDS entry %q: %w", trimmed, err)
		}

		if err := a.sqlcDB.UpsertPlatformAdmin(ctx, queries.UpsertPlatformAdminParams{
			UserID: userID,
			Role:   string(authtypes.PlatformRoleSuperAdmin),
		}); err != nil {
			return fmt.Errorf("upsert super admin %s: %w", userID, err)
		}
	}

	return nil
}
