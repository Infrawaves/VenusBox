package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var ErrUnknownIssuer = errors.New("no identity directory registered for issuer")

var ErrUserNotFound = errors.New("user not found")

// ErrIdentityNotFound means the directory has no identity for the subject (404).
var ErrIdentityNotFound = errors.New("identity not found")

// Directory is the subject-keyed admin API of a single identity provider
// (e.g. one Ory project). It never touches the database; issuer routing is the
// Service's concern.
type Directory interface {
	GetIdentity(ctx context.Context, subject string) (Identity, error)
	ListIdentities(ctx context.Context, subjects []string) ([]Identity, error)
	SearchByEmail(ctx context.Context, email string) ([]Identity, error)
	// SearchByProfile returns identities whose email or name contains query
	// (case-insensitive), scanning the directory locally, up to limit matches.
	SearchByProfile(ctx context.Context, query string, limit int) ([]Identity, error)
	SetExternalID(ctx context.Context, subject string, externalID uuid.UUID) error
	DeleteIdentity(ctx context.Context, subject string) error
}
