// tos-ref-count-consumer is a standalone CLI wrapper over packages/orchestrator/pkg/tosrefcount that deploys and scales independently of the orchestrator binary (see the "tos-ref-count-consumer" Nomad job). It has two uses:
//
//   - `consume`: the production entry point, running the live event/soft-delete loops under a Redis-backed leader lock (see pkg/tosrefcount.RunElected), so more than one replica is safe to run.
//   - `backfill`: the historical-backfill path from tos-reference-counting-plan.md §6, applying tos-layer-scan's JSON Lines output against env_builds.env_id with source='backfill_scan'.
//
// Configuration via environment variables:
//
//	REDIS_URL                     Redis address (default "localhost:6379"), required for `consume`
//	POSTGRES_CONNECTION_STRING    Postgres DSN, required for both subcommands
//	TOS_REF_COUNT_TEAM_FILTER     Optional comma-separated team_id uuids; restricts `consume`'s soft-delete scan to those teams when set (event consumption is never filtered; not used by `backfill`)
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
	"strings"
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
		//nolint:gocritic // process exits immediately after; skipping stop() is harmless
		log.Fatalf("unknown subcommand %q: usage: tos-ref-count-consumer <consume|backfill> [flags]", os.Args[1])
	}
}

func runConsume(ctx context.Context) {
	teamFilter, err := parseTeamFilter(os.Getenv("TOS_REF_COUNT_TEAM_FILTER"))
	if err != nil {
		log.Fatalf("invalid TOS_REF_COUNT_TEAM_FILTER: %s", err)
	}

	redisURL := getenv("REDIS_URL", "localhost:6379")
	pool := mustConnectPostgres(ctx)
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisURL})
	defer rdb.Close()

	consumer := tosrefcount.New(pool, teamFilter)
	locker := &redis_utils.RedisLocker{Client: redislock.New(rdb)}

	if len(teamFilter) > 0 {
		log.Printf("tos-ref-count-consumer: soft-delete scan restricted to team_ids=%v (event consumption is not filtered)", teamFilter)
	} else {
		log.Printf("tos-ref-count-consumer: no team filter, soft-delete scan processes all teams")
	}
	log.Printf("tos-ref-count-consumer started, redis=%s list=%s", redisURL, tosrefcount.EventsListKey)

	errCh := make(chan error, 2)
	go func() {
		errCh <- tosrefcount.RunElected(ctx, locker, "events", func(leaseCtx context.Context) error {
			return consumer.ConsumeEvents(leaseCtx, rdb)
		})
	}()
	go func() {
		errCh <- tosrefcount.RunElected(ctx, locker, "soft-delete", func(leaseCtx context.Context) error {
			return consumer.RunSoftDeleteScanner(leaseCtx, 30*time.Second)
		})
	}()

	select {
	case <-ctx.Done():
		log.Println("shutting down")
	case err := <-errCh:
		if err != nil {
			//nolint:gocritic // process exits immediately after; skipping the deferred closes is harmless
			log.Fatalf("consumer loop exited: %s", err)
		}
	}
}

// backfillBatchSize bounds how many input lines share one transaction. ApplyUploadEventTx issues 2-3 round trips
// per ancestor, so one-event-per-transaction (as a naive per-line Begin/Commit would do) spends most of its wall
// time on commit latency rather than the inserts themselves; batching amortizes that cost across many lines while
// keeping each transaction (and the row-level locks it holds) bounded in size and retry cost.
const backfillBatchSize = 500

func runBackfill(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("backfill", flag.ExitOnError)
	inputFlag := fs.String("input", "", "path to tos-layer-scan JSON Lines output")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse flags: %s", err)
	}
	if *inputFlag == "" {
		log.Fatal("-input is required (path to tos-layer-scan JSON Lines output)")
	}

	f, err := os.Open(*inputFlag)
	if err != nil {
		log.Fatalf("open input: %s", err)
	}
	defer f.Close()

	pool := mustConnectPostgres(ctx)
	defer pool.Close()

	var processed, failed int
	var batch [][]byte
	scanner := bufio.NewScanner(f)
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		if ctx.Err() != nil {
			//nolint:gocritic // process exits immediately after; skipping the deferred closes is harmless
			log.Fatal("interrupted")
		}

		batch = append(batch, append([]byte(nil), scanner.Bytes()...))
		if len(batch) >= backfillBatchSize {
			p, fl := runBackfillBatch(ctx, pool, batch)
			processed += p
			failed += fl
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		//nolint:gocritic // process exits immediately after; skipping the deferred closes is harmless
		log.Fatalf("read input: %s", err)
	}
	if len(batch) > 0 {
		p, fl := runBackfillBatch(ctx, pool, batch)
		processed += p
		failed += fl
	}

	log.Printf("backfill complete: %d build(s) processed, %d failed", processed, failed)
}

// runBackfillBatch applies every line in batch within a single transaction, logging and skipping (not aborting the
// batch for) any line that fails to parse or whose header wasn't read — matching backfillLine's prior per-line
// semantics, just sharing one commit across the whole batch instead of paying one per line.
func runBackfillBatch(ctx context.Context, pool *pgxpool.Pool, batch [][]byte) (processed, failed int) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatalf("begin batch tx: %s", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback is a no-op after Commit

	for _, line := range batch {
		if err := backfillLineTx(ctx, tx, line); err != nil {
			log.Printf("backfill line failed, skipping: %s", err)
			failed++

			continue
		}
		processed++
	}

	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("commit backfill batch: %s", err)
	}

	return processed, failed
}

// scanLine mirrors cmd/tos-layer-scan's buildResult/artifactResult output shape, kept as an independent copy since the two tools talk over a file format, not a Go import.
type scanLine struct {
	BuildID   string `json:"build_id"`
	Artifacts []struct {
		Artifact       string   `json:"artifact"`
		HeaderFound    bool     `json:"header_found"`
		HeaderError    string   `json:"header_error,omitempty"`
		AncestorBuilds []string `json:"ancestor_builds,omitempty"`
	} `json:"artifacts"`
}

// backfillLineTx applies one tos-layer-scan JSON Lines row within tx: unions ancestor_builds across the build's artifacts, looks up its owning env_id from env_builds, and writes the edges.
//
// Every artifact's header must have been read successfully. A missing/unreadable header (HeaderFound=false) is treated as a failure for the whole line, not a partial success — tos-layer-scan reports every build's memfile and rootfs header as present (see inspectArtifact), so a false HeaderFound here means the ancestors unique to that artifact were never counted, and silently continuing would make the edge set (and any future reclamation decision based on it) wrong rather than merely incomplete.
func backfillLineTx(ctx context.Context, tx pgx.Tx, line []byte) error {
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
		if !artifact.HeaderFound {
			return fmt.Errorf("build %s: artifact %q header not read: %s", buildID, artifact.Artifact, artifact.HeaderError)
		}
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

	envID, err := lookupEnvIDTx(ctx, tx, buildID)
	if err != nil {
		return fmt.Errorf("lookup env_id for build %s: %w", buildID, err)
	}

	return tosrefcount.ApplyUploadEventTx(ctx, tx, buildID, ancestorIDs, envID, "backfill_scan")
}

// lookupEnvIDTx reads env_builds.env_id for buildID within tx, returning "" (not an error) when the build has no env_builds row — the normal case for intermediate ancestor builds in a dependency chain.
func lookupEnvIDTx(ctx context.Context, tx pgx.Tx, buildID uuid.UUID) (string, error) {
	var envID *string
	err := tx.QueryRow(ctx, `SELECT env_id FROM env_builds WHERE id = $1`, buildID).Scan(&envID)
	switch {
	case errors.Is(err, pgx.ErrNoRows): //nolint:forbidigo // this cmd is its own module and doesn't depend on packages/db
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

// parseTeamFilter parses a comma-separated list of team_id uuids, returning nil for an empty string (no filter, process every team); any entry that fails to parse is a fatal config error, not silently ignored.
func parseTeamFilter(s string) ([]uuid.UUID, error) {
	if s == "" {
		return nil, nil
	}

	parts := strings.Split(s, ",")
	ids := make([]uuid.UUID, 0, len(parts))
	for _, p := range parts {
		id, err := uuid.Parse(strings.TrimSpace(p))
		if err != nil {
			return nil, fmt.Errorf("parse team_id %q: %w", p, err)
		}
		ids = append(ids, id)
	}

	return ids, nil
}
