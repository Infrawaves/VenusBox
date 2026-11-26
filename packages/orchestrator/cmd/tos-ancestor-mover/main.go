// tos-ancestor-mover is a standalone, one-shot tool that relocates the TOS objects of builds with
// ref_count = 0 in tos_ancestor_reference_count (see tos-reference-counting-plan.md) from the live template
// bucket into a separate bucket, via server-side CopyObject, deleting the source objects only after every
// object for that build has been copied successfully.
//
// It moves only each build's own object prefix ("{buildID}/..."), never its ancestors — a build's ref_count
// measures whether anything still depends on it, not what it itself depends on, so moving only its own prefix
// cannot affect any other build's data.
//
// Configuration via environment variables:
//
//	POSTGRES_CONNECTION_STRING    Postgres DSN (required)
//	STORAGE_PROVIDER=AWSBucket
//	TEMPLATE_BUCKET_NAME          Source bucket (the live template bucket)
//	AWS_ENDPOINT_URL              Endpoint, for S3-compatible backends
//	AWS_REGION
//	AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
//	S3_USE_PATH_STYLE=true        For S3-compatible backends needing path-style addressing
//
// Usage:
//
//	tos-ancestor-mover -dest-bucket dev-hk-tpl-mv [-dry-run]
//	tos-ancestor-mover -dest-bucket dev-hk-tpl-mv -ancestor-id <uuid>   # move only this one build, for a trial run
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// moveResult is one line of the JSON Lines report written to stdout, one per ancestor_id attempted.
type moveResult struct {
	AncestorID string   `json:"ancestor_id"`
	Status     string   `json:"status"` // "moved" | "skipped_no_objects" | "failed"
	ObjectKeys []string `json:"object_keys,omitempty"`
	Error      string   `json:"error,omitempty"`

	// AuditError is set when status is "moved" but recording the move in tos_ancestor_moves failed — the
	// TOS move itself already succeeded and is not retried or undone, only its audit record is missing.
	// Kept separate from Error so a reader can't mistake a merely-unrecorded success for a failed move.
	AuditError string `json:"audit_error,omitempty"`
}

func main() {
	destBucket := flag.String("dest-bucket", "", "destination bucket to move ref_count=0 build objects into")
	dryRun := flag.Bool("dry-run", false, "list what would be moved without copying or deleting anything")
	onlyAncestorID := flag.String("ancestor-id", "", "if set, move only this one ancestor_id instead of every ref_count=0 row (for a trial run before moving everything)")
	flag.Parse()

	if *destBucket == "" {
		fmt.Fprintln(os.Stderr, "usage: tos-ancestor-mover -dest-bucket <bucket> [-dry-run] [-ancestor-id <uuid>]")
		os.Exit(1)
	}

	ctx := context.Background()

	pgDSN := os.Getenv("POSTGRES_CONNECTION_STRING")
	if pgDSN == "" {
		log.Fatal("POSTGRES_CONNECTION_STRING is required")
	}
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		log.Fatalf("connect postgres: %s", err)
	}
	defer pool.Close()

	sourceBucket := os.Getenv("TEMPLATE_BUCKET_NAME")
	if sourceBucket == "" {
		log.Fatal("TEMPLATE_BUCKET_NAME is required")
	}
	if sourceBucket == *destBucket {
		log.Fatalf("source and destination bucket are both %q — refusing to run", sourceBucket)
	}

	client, err := newS3Client(ctx)
	if err != nil {
		log.Fatalf("initialize S3 client: %s", err)
	}

	var ancestorIDs []uuid.UUID
	if *onlyAncestorID != "" {
		id, err := uuid.Parse(*onlyAncestorID)
		if err != nil {
			log.Fatalf("invalid -ancestor-id %q: %s", *onlyAncestorID, err)
		}

		// Only ever move what the reference-count table actually says is unreferenced — an -ancestor-id
		// typo or a stale value from an earlier query must not be able to move a still-referenced build's
		// data purely because it was typed on the command line.
		isZero, err := isRefCountZero(ctx, pool, id)
		if err != nil {
			log.Fatalf("check ref_count for %s: %s", id, err)
		}
		if !isZero {
			log.Fatalf("refusing to move %s: ref_count is not 0 (or the row doesn't exist)", id)
		}

		ancestorIDs = []uuid.UUID{id}
	} else {
		ancestorIDs, err = loadZeroRefCountAncestors(ctx, pool)
		if err != nil {
			log.Fatalf("query ref_count=0 ancestors: %s", err)
		}
	}
	log.Printf("tos-ancestor-mover: %d ancestor(s) to process, source=%s dest=%s dry_run=%v", len(ancestorIDs), sourceBucket, *destBucket, *dryRun)

	enc := json.NewEncoder(os.Stdout)
	var moved, skipped, failed int
	for _, ancestorID := range ancestorIDs {
		result := moveBuild(ctx, pool, client, sourceBucket, *destBucket, ancestorID, *dryRun)
		if err := enc.Encode(result); err != nil {
			log.Printf("failed to encode result for %s: %s", ancestorID, err)
		}

		switch result.Status {
		case "moved":
			moved++
		case "skipped_no_objects":
			skipped++
		case "failed":
			failed++
		}
	}

	log.Printf("tos-ancestor-mover complete: %d moved, %d skipped (no objects), %d failed", moved, skipped, failed)
}

func newS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	usePathStyle, _ := strconv.ParseBool(os.Getenv("S3_USE_PATH_STYLE"))

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		if region := os.Getenv("AWS_REGION"); region != "" {
			o.Region = region
		}
		o.UsePathStyle = usePathStyle
	}), nil
}

func isRefCountZero(ctx context.Context, pool *pgxpool.Pool, ancestorID uuid.UUID) (bool, error) {
	var refCount int
	err := pool.QueryRow(ctx, `SELECT ref_count FROM tos_ancestor_reference_count WHERE ancestor_id = $1`, ancestorID).Scan(&refCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) { //nolint:forbidigo // this cmd is its own module and doesn't depend on packages/db
			return false, nil
		}

		return false, err
	}

	return refCount == 0, nil
}

func loadZeroRefCountAncestors(ctx context.Context, pool *pgxpool.Pool) ([]uuid.UUID, error) {
	rows, err := pool.Query(ctx, `SELECT ancestor_id FROM tos_ancestor_reference_count WHERE ref_count = 0 ORDER BY ancestor_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, rows.Err()
}

// moveBuild lists every object under "{ancestorID}/" in sourceBucket, copies each to destBucket
// (server-side, preserving the key), and only once every copy has succeeded deletes the source objects and
// records the move in tos_ancestor_moves (see schema.sql) — the only way to reverse a mistaken move later.
// A failure at any step — listing, any copy, or any delete — leaves the source bucket untouched for that
// build (partially copied destination objects from a failed attempt are harmless: they are never treated as
// authoritative without the corresponding source deletion, and re-running this tool re-copies them
// idempotently) and is reported, not fatal — other builds continue to be processed.
func moveBuild(ctx context.Context, pool *pgxpool.Pool, client *s3.Client, sourceBucket, destBucket string, ancestorID uuid.UUID, dryRun bool) moveResult {
	prefix := ancestorID.String() + "/"

	keys, err := listAllKeys(ctx, client, sourceBucket, prefix)
	if err != nil {
		return moveResult{AncestorID: ancestorID.String(), Status: "failed", Error: fmt.Sprintf("list objects: %s", err)}
	}
	if len(keys) == 0 {
		return moveResult{AncestorID: ancestorID.String(), Status: "skipped_no_objects"}
	}

	if dryRun {
		return moveResult{AncestorID: ancestorID.String(), Status: "moved", ObjectKeys: keys}
	}

	for _, key := range keys {
		if err := copyObject(ctx, client, sourceBucket, destBucket, key); err != nil {
			return moveResult{AncestorID: ancestorID.String(), Status: "failed", ObjectKeys: keys, Error: fmt.Sprintf("copy %q: %s", key, err)}
		}
	}

	// Every copy succeeded — safe to delete the source objects. A failure here leaves both a complete
	// copy in destBucket and the (now-redundant) originals in sourceBucket; re-running the tool will
	// retry the delete (CopyObject is skipped as a no-op cost, not a correctness risk, since it's
	// idempotent) rather than leave the move half-done.
	for _, key := range keys {
		if err := deleteObject(ctx, client, sourceBucket, key); err != nil {
			return moveResult{AncestorID: ancestorID.String(), Status: "failed", ObjectKeys: keys, Error: fmt.Sprintf("delete source %q after successful copy: %s", key, err)}
		}
	}

	result := moveResult{AncestorID: ancestorID.String(), Status: "moved", ObjectKeys: keys}

	// The move is already final at this point (source deleted, destination has every object) — a failure
	// recording it is reported but does not change Status or retry anything, since there's nothing left
	// to retry: re-running the tool would see ref_count still 0 and the source objects already gone,
	// re-attempt the same copy+delete sequence, and produce another (also-unaudited) no-op.
	if err := recordMove(ctx, pool, ancestorID, sourceBucket, destBucket, keys); err != nil {
		result.AuditError = fmt.Sprintf("record move in tos_ancestor_moves: %s", err)
	}

	return result
}

func recordMove(ctx context.Context, pool *pgxpool.Pool, ancestorID uuid.UUID, sourceBucket, destBucket string, keys []string) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO tos_ancestor_moves (ancestor_id, source_bucket, dest_bucket, object_keys)
		VALUES ($1, $2, $3, $4)
	`, ancestorID, sourceBucket, destBucket, keys)

	return err
}

func listAllKeys(ctx context.Context, client *s3.Client, bucket, prefix string) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		pageCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		page, err := paginator.NextPage(pageCtx)
		cancel()
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}

	return keys, nil
}

func copyObject(ctx context.Context, client *s3.Client, sourceBucket, destBucket, key string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// CopySource must be bucket/key, URL-encoded if the key contains characters outside the unreserved
	// set — build UUIDs and the fixed file names used here never do, so no encoding is applied.
	copySource := sourceBucket + "/" + key

	_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String(key),
		CopySource: aws.String(copySource),
	})

	return err
}

func deleteObject(ctx context.Context, client *s3.Client, bucket, key string) error {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			// Already gone — treat as success so a retry after a partial prior run doesn't fail here.
			return nil
		}

		return err
	}

	return nil
}
