// tos-ref-count-consumer is a standalone CLI wrapper over
// packages/orchestrator/pkg/tosrefcount, the TOS layer reference-counting
// consumer. The orchestrator binary now runs this logic in-process under the
// template-manager role (packages/orchestrator/pkg/factories/run.go,
// setupTOSRefCount) whenever TOS_REF_COUNT_POSTGRES_CONNECTION_STRING is
// set, so this binary is no longer required for normal operation. It still
// has two uses:
//
//   - `consume`: run the same live event/soft-delete loops standalone, for
//     local dev against a Redis/Postgres pair without a full orchestrator,
//     or as a manual stand-in if the in-process consumer needs to be
//     bypassed. It takes the same Redis-backed leader lock as the
//     in-process consumer (see pkg/tosrefcount.RunElected), so running this
//     alongside orchestrator's own consumer is safe — at most one of them
//     is ever active.
//   - `backfill`: the historical-backfill path from
//     tos-reference-counting-plan.md §6. Reads tos-layer-scan's JSON Lines
//     output (one buildResult per line, each listing ancestor_builds per
//     artifact), looks up each build's owning env_id from env_builds, and
//     writes edges with source='backfill_scan'.
//
// Configuration via environment variables:
//
//	REDIS_URL                     Redis address (default "localhost:6379"), required for `consume`
//	POSTGRES_CONNECTION_STRING    Postgres DSN, required for both subcommands
//
// Usage:
//
//	tos-ref-count-consumer consume
//	tos-ref-count-consumer backfill -input result.jsonl
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bsm/redislock"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/tosrefcount"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: tos-ref-count-consumer <consume|backfill> [flags]")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "consume":
		runConsume(ctx)
	case "backfill":
		runBackfill(ctx, os.Args[2:])
	default:
		log.Fatalf("unknown subcommand %q: usage: tos-ref-count-consumer <consume|backfill> [flags]", os.Args[1])
	}
}

func runConsume(ctx context.Context) {
	redisURL := getenv("REDIS_URL", "localhost:6379")
	pool := mustConnectPostgres(ctx)
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisURL})
	defer rdb.Close()

	consumer := tosrefcount.New(pool)
	locker := &redis_utils.RedisLocker{Client: redislock.New(rdb)}

	log.Printf("tos-ref-count-consumer started, redis=%s list=%s", redisURL, tosrefcount.EventsListKey)

	errCh := make(chan error, 2)
	go func() {
		errCh <- tosrefcount.RunElected(ctx, locker, func(leaseCtx context.Context) error {
			return consumer.ConsumeEvents(leaseCtx, rdb)
		})
	}()
	go func() {
		errCh <- tosrefcount.RunElected(ctx, locker, func(leaseCtx context.Context) error {
			return consumer.RunSoftDeleteScanner(leaseCtx, 30*time.Second)
		})
	}()

	select {
	case <-ctx.Done():
		log.Println("shutting down")
	case err := <-errCh:
		if err != nil {
			log.Fatalf("consumer loop exited: %s", err)
		}
	}
}

func runBackfill(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("backfill", flag.ExitOnError)
	inputFlag := fs.String("input", "", "path to tos-layer-scan JSON Lines output")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse flags: %s", err)
	}
	if *inputFlag == "" {
		log.Fatal("-input is required (path to tos-layer-scan JSON Lines output)")
	}

	pool := mustConnectPostgres(ctx)
	defer pool.Close()

	f, err := os.Open(*inputFlag)
	if err != nil {
		log.Fatalf("open input: %s", err)
	}
	defer f.Close()

	consumer := tosrefcount.New(pool)

	var processed, failed int
	scanner := bufio.NewScanner(f)
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		if ctx.Err() != nil {
			log.Fatal("interrupted")
		}

		if err := backfillLine(ctx, pool, consumer, scanner.Bytes()); err != nil {
			log.Printf("backfill line failed, skipping: %s", err)
			failed++

			continue
		}
		processed++
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("read input: %s", err)
	}

	log.Printf("backfill complete: %d build(s) processed, %d failed", processed, failed)
}

// scanLine mirrors cmd/tos-layer-scan's buildResult/artifactResult output
// shape. Kept as an independent copy rather than a shared type for the same
// reason as tosrefcount.Event: the two tools talk over a file format, not a
// Go import.
type scanLine struct {
	BuildID   string `json:"build_id"`
	Artifacts []struct {
		AncestorBuilds []string `json:"ancestor_builds,omitempty"`
	} `json:"artifacts"`
}

// backfillLine applies one tos-layer-scan JSON Lines row: unions
// ancestor_builds across that build's artifacts (memfile and rootfs.ext4
// cover the same dependency chain per tos-reference-counting-plan.md §1,
// so either one, or both, contribute to the same edge set), looks up the
// build's owning env_id from env_builds, and writes the edges.
func backfillLine(ctx context.Context, pool *pgxpool.Pool, consumer *tosrefcount.Consumer, line []byte) error {
	var row scanLine
	if err := json.Unmarshal(line, &row); err != nil {
		return fmt.Errorf("unmarshal line: %w", err)
	}

	buildID, err := uuid.Parse(row.BuildID)
	if err != nil {
		return fmt.Errorf("parse build_id %q: %w", row.BuildID, err)
	}

	seen := make(map[uuid.UUID]struct{})
	var ancestorIDs []uuid.UUID
	for _, artifact := range row.Artifacts {
		for _, s := range artifact.AncestorBuilds {
			id, err := uuid.Parse(s)
			if err != nil {
				return fmt.Errorf("parse ancestor_id %q: %w", s, err)
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ancestorIDs = append(ancestorIDs, id)
		}
	}
	if len(ancestorIDs) == 0 {
		return fmt.Errorf("build %s: no ancestor_builds in any artifact", buildID)
	}

	envID, err := lookupEnvID(ctx, pool, buildID)
	if err != nil {
		return fmt.Errorf("lookup env_id for build %s: %w", buildID, err)
	}

	return consumer.ApplyUploadEvent(ctx, buildID, envID, "backfill", ancestorIDs, "backfill_scan")
}

// lookupEnvID reads env_builds.env_id for buildID. Returns "" (not an
// error) when the build has no env_builds row at all — this is the
// expected, normal case for the intermediate ancestor builds in a
// dependency chain (tos-reference-counting-plan.md §1: "很多根本不在
// env_builds 表里登记"), not a data problem to fail the whole line over.
func lookupEnvID(ctx context.Context, pool *pgxpool.Pool, buildID uuid.UUID) (string, error) {
	var envID *string
	err := pool.QueryRow(ctx, `SELECT env_id FROM env_builds WHERE id = $1`, buildID).Scan(&envID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "", nil
	case err != nil:
		return "", err
	case envID == nil:
		return "", nil
	default:
		return *envID, nil
	}
}

func mustConnectPostgres(ctx context.Context) *pgxpool.Pool {
	pgDSN := os.Getenv("POSTGRES_CONNECTION_STRING")
	if pgDSN == "" {
		log.Fatal("POSTGRES_CONNECTION_STRING is required")
	}

	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		log.Fatalf("connect postgres: %s", err)
	}

	return pool
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}
