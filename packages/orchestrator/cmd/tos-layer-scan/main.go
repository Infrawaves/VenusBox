// tos-layer-scan is a standalone, read-only tool for analyzing template-storage
// layer dependencies at scale. Given a list of build IDs, it reads each build's
// memfile.header and rootfs.ext4.header from the template storage bucket and
// reports which ancestor build IDs are referenced via header.Mapping.Builds().
//
// It does not modify anything in storage or in the database. It is not part of
// the orchestrator binary and does not touch any existing orchestrator code —
// it only imports the shared storage and storage/header packages that already
// back production reads.
//
// Configuration is via standard environment variables, matching how the
// orchestrator/template-manager configure storage:
//
//	STORAGE_PROVIDER=AWSBucket
//	TEMPLATE_BUCKET_NAME=<bucket>
//	AWS_ENDPOINT_URL=<endpoint, for S3-compatible backends>
//	AWS_REGION=<region>
//	AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
//	S3_USE_PATH_STYLE=true   (for S3-compatible backends needing path-style addressing)
//
// Usage:
//
//	tos-layer-scan -build <uuid>                 # inspect a single build
//	tos-layer-scan -input ids.txt -out result.jsonl -workers 32
//	  # ids.txt: one build UUID per line
//	  # result.jsonl: one JSON object per line (JSON Lines), safe to stream for
//	  #   millions of rows without holding everything in memory
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

type artifactResult struct {
	Artifact       string   `json:"artifact"` // "memfile" or "rootfs.ext4"
	HeaderFound    bool     `json:"header_found"`
	HeaderError    string   `json:"header_error,omitempty"`
	Version        uint64   `json:"version,omitempty"`
	MappingCount   int      `json:"mapping_count,omitempty"`
	AncestorBuilds []string `json:"ancestor_builds,omitempty"`
}

type buildResult struct {
	BuildID   string           `json:"build_id"`
	Artifacts []artifactResult `json:"artifacts"`
}

func main() {
	buildFlag := flag.String("build", "", "single build ID to inspect")
	inputFlag := flag.String("input", "", "path to a file with one build ID per line (for batch mode)")
	outFlag := flag.String("out", "", "path to write JSON Lines output (batch mode only; defaults to stdout)")
	workersFlag := flag.Int("workers", 16, "number of concurrent workers (batch mode only)")
	flag.Parse()

	if *buildFlag == "" && *inputFlag == "" {
		fmt.Fprintln(os.Stderr, "usage: tos-layer-scan -build <uuid>  OR  tos-layer-scan -input ids.txt -out result.jsonl [-workers 32]")
		os.Exit(1)
	}

	ctx := context.Background()

	provider, err := storage.GetStorageProvider(ctx, storage.TemplateStorageConfig)
	if err != nil {
		log.Fatalf("failed to initialize storage provider: %s", err)
	}

	if *buildFlag != "" {
		result := inspectBuild(ctx, provider, *buildFlag)
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(result)

		return
	}

	runBatch(ctx, provider, *inputFlag, *outFlag, *workersFlag)
}

func runBatch(ctx context.Context, provider storage.StorageProvider, inputPath, outPath string, workers int) {
	ids, err := readIDs(inputPath)
	if err != nil {
		log.Fatalf("failed to read input file: %s", err)
	}

	out := os.Stdout
	if outPath != "" {
		f, err := os.Create(outPath)
		if err != nil {
			log.Fatalf("failed to create output file: %s", err)
		}
		defer f.Close()
		out = f
	}

	var (
		writeMu sync.Mutex
		wg      sync.WaitGroup
		sem     = make(chan struct{}, workers)
		done    = 0
		doneMu  sync.Mutex
	)

	enc := json.NewEncoder(out)

	for _, id := range ids {
		wg.Add(1)
		sem <- struct{}{}

		go func(buildID string) {
			defer wg.Done()
			defer func() { <-sem }()

			result := inspectBuild(ctx, provider, buildID)

			writeMu.Lock()
			_ = enc.Encode(result)
			writeMu.Unlock()

			doneMu.Lock()
			done++
			if done%1000 == 0 {
				fmt.Fprintf(os.Stderr, "[progress] %d/%d\n", done, len(ids))
			}
			doneMu.Unlock()
		}(id)
	}

	wg.Wait()
	fmt.Fprintf(os.Stderr, "[done] processed %d build IDs\n", len(ids))
}

func readIDs(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ids []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		ids = append(ids, line)
	}

	return ids, scanner.Err()
}

// inspectBuild reads both the memfile and rootfs headers for a build and
// reports the ancestor build IDs referenced by each. A header that is
// missing or fails to parse is reported as such (HeaderFound=false) rather
// than treated as "no dependencies" — callers must fail closed on this case.
func inspectBuild(ctx context.Context, provider storage.StorageProvider, buildID string) buildResult {
	paths := storage.Paths{BuildID: buildID}

	return buildResult{
		BuildID: buildID,
		Artifacts: []artifactResult{
			inspectArtifact(ctx, provider, "memfile", paths.MemfileHeader()),
			inspectArtifact(ctx, provider, "rootfs.ext4", paths.RootfsHeader()),
		},
	}
}

func inspectArtifact(ctx context.Context, provider storage.StorageProvider, artifactName, headerPath string) artifactResult {
	result := artifactResult{Artifact: artifactName}

	h, _, err := header.LoadHeader(ctx, provider, headerPath)
	if err != nil {
		result.HeaderError = err.Error()

		return result
	}

	result.HeaderFound = true
	result.Version = h.Metadata.Version
	result.MappingCount = h.Mapping.Len()

	for _, id := range h.Mapping.Builds() {
		if id == uuid.Nil {
			continue
		}
		result.AncestorBuilds = append(result.AncestorBuilds, id.String())
	}

	return result
}
