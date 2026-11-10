package tosrefcount

import (
	"context"
	"errors"
	"time"

	"github.com/bsm/redislock"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// leaderLockKey is a single, fixed key: build nodes run template-manager as
// a scalable pool (docs/ARCHITECTURE.md), not a singleton, and this consumer
// assumes exactly one process is ever writing ref_count at a time — two
// instances racing the same +1/-1 are individually idempotent but would
// double the soft-delete scan's query load for no benefit. RunElected holds
// this lock for as long as it is the active instance and keeps retrying
// acquisition in the background so another instance takes over within one
// TTL of a crash, without needing any coordination beyond Redis.
const leaderLockKey = "tos-ref-count-consumer-leader"

const (
	leaderLockTTL         = 30 * time.Second
	leaderLockRefresh     = 10 * time.Second
	leaderLockRetryPeriod = 5 * time.Second
)

// RunElected runs fn only while this process holds the distributed leader
// lock, retrying acquisition every leaderLockRetryPeriod until ctx is
// canceled. fn is expected to block until its ctx is canceled; RunElected
// cancels that ctx and waits for fn to return before releasing the lock,
// so a lost lock (e.g. a refresh failure under Redis unavailability) always
// stops fn before another instance can acquire the lock and start its own.
func RunElected(ctx context.Context, locker redis_utils.Locker, fn func(context.Context) error) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		lock, err := locker.Obtain(ctx, redis_utils.GetLockKey(leaderLockKey), leaderLockTTL, nil)
		if err != nil {
			if !errors.Is(err, redislock.ErrNotObtained) {
				logger.L().Warn(ctx, "tos-ref-count: leader lock obtain error, retrying", zap.Error(err))
			}

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(leaderLockRetryPeriod):
			}

			continue
		}

		logger.L().Info(ctx, "tos-ref-count: acquired leader lock, starting consumer")

		if err := runWithLease(ctx, lock, fn); err != nil {
			return err
		}

		logger.L().Info(ctx, "tos-ref-count: lost leader lock, will retry acquisition")
	}
}

// runWithLease runs fn under a background refresh of lock, stopping fn as
// soon as either fn returns or the refresh loop fails to extend the lock
// before it expires. The lock is always released on the way out so the next
// RunElected iteration (this process or another) can acquire it promptly
// rather than waiting out the full TTL.
func runWithLease(ctx context.Context, lock redis_utils.Lock, fn func(context.Context) error) error {
	leaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	refreshErrCh := make(chan error, 1)
	go func() {
		refreshErrCh <- refreshLockUntilDone(leaseCtx, lock)
	}()

	fnErrCh := make(chan error, 1)
	go func() {
		fnErrCh <- fn(leaseCtx)
	}()

	var fnErr error
	select {
	case fnErr = <-fnErrCh:
		cancel()
		<-refreshErrCh
	case <-refreshErrCh:
		// Lost the lease — stop fn immediately so this process never keeps
		// writing after another instance may have acquired the lock.
		cancel()
		fnErr = <-fnErrCh
	}

	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer releaseCancel()
	if err := lock.Release(releaseCtx); err != nil && !errors.Is(err, redislock.ErrLockNotHeld) {
		logger.L().Warn(ctx, "tos-ref-count: leader lock release error", zap.Error(err))
	}

	return fnErr
}

// refreshLockUntilDone extends lock every leaderLockRefresh until ctx is
// canceled (returns nil) or a refresh fails (returns the error), which the
// caller treats as losing the lease.
func refreshLockUntilDone(ctx context.Context, lock redis_utils.Lock) error {
	refreshable, ok := lock.(interface {
		Refresh(ctx context.Context, ttl time.Duration, opt *redislock.Options) error
	})
	if !ok {
		// Locker implementations used outside production (e.g. test doubles)
		// may not support refresh; treat the lease as held for ctx's
		// lifetime rather than failing a caller that never set one up.
		<-ctx.Done()

		return nil
	}

	ticker := time.NewTicker(leaderLockRefresh)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := refreshable.Refresh(ctx, leaderLockTTL, nil); err != nil {
				return err
			}
		}
	}
}
