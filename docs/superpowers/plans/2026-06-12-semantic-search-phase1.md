# Semantic Search (Phase 1 — SQLite) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in hybrid (lexical + vector) semantic search to kata on the SQLite backend, plus all backend-neutral machinery, degrading to today's FTS behavior whenever embeddings are absent or the embedder is unreachable.

**Architecture:** An OpenAI-compatible embedding endpoint (configured in `config.toml`) produces one L2-normalized vector per issue over `title + "\n\n" + body`. A daemon reconciler keeps a canonical `issue_embeddings` table fresh, driven by a precise `issues.content_revision` counter. `kata search` runs FTS and a brute-force cosine vector leg concurrently and merges them with reciprocal rank fusion; it falls back to lexical-only (labeled `degraded`) when the vector leg cannot run. Embeddings are local derived state and do not federate.

**Tech Stack:** Go, `modernc.org/sqlite` (pure-Go driver), huma v2 HTTP framework, cobra CLI, BurntSushi TOML. No new third-party dependencies.

**Scope note:** This plan is Phase 1 only. **Phase 2 (PostgreSQL parity: implement `SearchFTS`/`SearchFTSAny` over the existing tsvector machinery, the pgvector acceleration path, and extend the storage conformance suite) is a separate follow-up plan.** The design note is `docs/design/semantic-search.md`.

---

## Reference facts (verified against the codebase)

- Module path: `go.kenn.io/kata`.
- Schema version constant: `currentSchemaVersion = 16` at `internal/db/schema_version.go:8`. **This plan bumps it to 17.** Old DBs upgrade via JSONL cutover (export → fresh schema → import); there is no in-place migration. `schema.sql` is the single source of truth applied atomically by `bootstrapOnce` (`internal/db/sqlitestore/store.go:169-213`).
- API schema version: `const APISchemaVersion = "0.2.0"` at `internal/daemon/openapi.go:15`. **This plan bumps it to "0.3.0".**
- Agent format version: `agentFormatVersion = 1` at `cmd/kata/output_mode.go`. **Stays 1** (appended fields are non-breaking per `docs/reference/agent-output.md`).
- Handler config struct: `ServerConfig` at `internal/daemon/server.go:27-52` (`DB db.Storage`, `Broadcaster *EventBroadcaster`, `Hooks hooks.Sink`, `StartedAt time.Time`, ...).
- Handlers registered via `huma.Register(humaAPI, huma.Operation{...}, fn)` inside `registerXxx(humaAPI huma.API, cfg ServerConfig)`, wired in `registerRoutes` (`internal/daemon/server.go:208-232`).
- Daemon goroutines launched in `runDaemonWithListen` (`cmd/kata/daemon_cmd.go:284-403`); pattern is `x := startXRunner(ctx, ...)` returning a wake func, or `go runner.Run(ctx)`. Broadcaster subscription pattern at `cmd/kata/daemon_cmd.go:481`.
- `Store` embeds `*sql.DB`; helpers: `nowTimestamp()` (`queries.go:1452`), `retryWrite3` (`store.go:343`), `issueByIDTx` (`queries.go:780`), `scanIssue` (`queries.go:1545`), `issueSelect` const (`queries.go:1543`), `issueFieldUpdatePlan` (`queries.go:1456-1501`).
- Bearer client: `config.ConfigureBearerClientWithTrust(c *http.Client, baseURL, token string, trustPrivateNetwork bool) error` (`internal/config/bearer.go:19`); origin pinning + plaintext safety enforced inside.
- Agent output helpers in `cmd/kata/list.go:184-235`: `agentRowField`, `agentRowFloatField`, `agentRowListField`, `writeAgentKVRow`; output modes in `cmd/kata/output_mode.go:19-25`.

---

## File structure

**New files:**

- `internal/embedding/recipe.go` — `EmbedText(title, body string) string`, `RecipeVersion`, `Fingerprint(model string, dims int, salt string) string`. No imports of `db`/`daemon`.
- `internal/embedding/recipe_test.go`
- `internal/embedding/client.go` — `Client`, `Config`, `New`, `Embed`, `Fingerprint`, `Dims`; `APIError` with `Definitive()` / `RetryAfter`.
- `internal/embedding/client_test.go`
- `internal/db/embedding_types.go` — `IssueEmbedding`, `EmbedTarget`, `IssueEmbeddingExport` types.
- `internal/db/sqlitestore/queries_embeddings.go` — `UpsertIssueEmbedding`, `ListEmbedTargets`, `SearchVector`, the vector cache.
- `internal/db/sqlitestore/queries_embeddings_test.go`
- `internal/db/sqlitestore/export_embeddings.go` — `ExportIssueEmbeddings`.
- `internal/daemon/rrf.go` — `mergeRRF`, `resolveMode`, mode/degraded enums (pure).
- `internal/daemon/rrf_test.go`
- `internal/daemon/reconciler.go` — `Reconciler`, `NewReconciler`, `Run`, `Wake`, `Health`, `ReconcilerHealth`.
- `internal/daemon/reconciler_test.go`
- `internal/daemon/hybrid_search.go` — `hybridSearch` orchestration used by the search handler.
- `internal/daemon/hybrid_search_test.go`

**Modified files:**

- `internal/db/schema_version.go` — bump to 17.
- `internal/db/sqlitestore/schema.sql` — `issues.content_revision` column; `issue_embeddings` table.
- `internal/db/sqlitestore/queries.go` — `contentFieldsChanged` helper; bump `content_revision` in `editIssue`.
- `internal/db/sqlitestore/queries_edit_atomic.go` — bump `content_revision` in the field-change branch.
- `internal/db/sqlitestore/imports.go` — bump `content_revision` in `updateImportedIssue` when title/body differ.
- `internal/db/storage.go` — add `UpsertIssueEmbedding`, `ListEmbedTargets`, `SearchVector`, `EmbeddingStats`, `ExportIssueEmbeddings` to the interface.
- `internal/db/pgstore/stubs_gen.go` — stub the new methods (Phase 2 implements them).
- `internal/db/export_types.go` — `IssueExport.ContentRevision`; `IssueEmbeddingExport`.
- `internal/db/sqlitestore/export.go` — select `content_revision` in `ExportIssues`.
- `internal/jsonl/types.go` — `KindIssueEmbedding` constant + order.
- `internal/jsonl/storage_export.go` — emit issue embeddings.
- `internal/jsonl/import.go` — map the new kind; carry `content_revision`.
- `internal/config/daemon_config.go` — `[search.embeddings]` section + validation + env overlay.
- `internal/daemon/server.go` — `ServerConfig` gains `Embedder *embedding.Client` and `ReconcilerHealth func() ReconcilerHealth`.
- `internal/daemon/handlers_search.go` — call `hybridSearch`.
- `internal/daemon/handlers_health.go` — add reconciler health fields.
- `internal/daemon/openapi.go` — bump `APISchemaVersion`.
- `internal/api/types.go` — `SearchRequest.Mode`; `SearchResponse` mode/degraded fields; `HealthResponse` reconciler fields.
- `cmd/kata/daemon_cmd.go` — construct client + reconciler, start the goroutine when configured, subscribe to broadcaster.
- `cmd/kata/search.go` — `--lexical`/`--hybrid`/`--semantic` flags; mode in URL; human + agent rendering.

---

## Task 1: Schema — `content_revision` column and `issue_embeddings` table

**Files:**
- Modify: `internal/db/schema_version.go:8`
- Modify: `internal/db/sqlitestore/schema.sql`
- Test: `internal/db/sqlitestore/schema_test.go` (new or existing)

- [ ] **Step 1: Write the failing test**

Create `internal/db/sqlitestore/schema_embeddings_test.go`:

```go
package sqlitestore

import (
	"context"
	"testing"
)

func TestSchemaHasEmbeddingSurface(t *testing.T) {
	ctx := context.Background()
	d := newTestStore(t) // existing test helper that opens a fresh in-memory/temp Store

	// issues.content_revision exists and defaults to 0.
	var dflt int
	row := d.QueryRowContext(ctx,
		`SELECT count(*) FROM pragma_table_info('issues') WHERE name='content_revision'`)
	if err := row.Scan(&dflt); err != nil {
		t.Fatalf("pragma_table_info issues: %v", err)
	}
	if dflt != 1 {
		t.Fatalf("issues.content_revision missing")
	}

	// issue_embeddings table exists with expected columns.
	want := map[string]bool{
		"issue_id": false, "embedded_content_revision": false, "embed_fingerprint": false,
		"dims": false, "vector_bytes": false, "updated_at": false,
	}
	rows, err := d.QueryContext(ctx, `SELECT name FROM pragma_table_info('issue_embeddings')`)
	if err != nil {
		t.Fatalf("pragma_table_info issue_embeddings: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		if _, ok := want[n]; ok {
			want[n] = true
		}
	}
	for col, seen := range want {
		if !seen {
			t.Errorf("issue_embeddings missing column %q", col)
		}
	}
}
```

If `newTestStore` does not exist, check `internal/db/sqlitestore/store_test.go` for the actual fresh-store helper name and use that.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/db/sqlitestore/ -run TestSchemaHasEmbeddingSurface -v`
Expected: FAIL — `issues.content_revision missing` (column and table not yet added).

- [ ] **Step 3: Add the column and table to `schema.sql`**

In `internal/db/sqlitestore/schema.sql`, add `content_revision` to the `issues` table definition (place it next to `revision`):

```sql
  revision INTEGER NOT NULL DEFAULT 0,
  content_revision INTEGER NOT NULL DEFAULT 0,
```

Then add the embeddings table (place near the FTS section, after the `issues` table and its indexes):

```sql
-- Semantic search: one L2-normalized vector per issue. Canonical derived
-- state; pgvector acceleration (Phase 2) is built from this, never instead.
CREATE TABLE issue_embeddings (
  issue_id                  INTEGER PRIMARY KEY REFERENCES issues(id) ON DELETE CASCADE,
  embedded_content_revision INTEGER NOT NULL,
  embed_fingerprint         TEXT NOT NULL CHECK (length(embed_fingerprint) = 64),
  dims                      INTEGER NOT NULL CHECK (dims > 0),
  vector_bytes              BLOB NOT NULL CHECK (length(vector_bytes) = dims * 4),
  updated_at                TEXT NOT NULL
);
CREATE INDEX idx_issue_embeddings_fingerprint ON issue_embeddings(embed_fingerprint);
```

- [ ] **Step 4: Bump the schema version**

In `internal/db/schema_version.go:8`, change:

```go
const currentSchemaVersion = 17
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/db/sqlitestore/ -run TestSchemaHasEmbeddingSurface -v`
Expected: PASS.

- [ ] **Step 6: Run the broader DB suite to catch schema-version assertions**

Run: `go test ./internal/db/... 2>&1 | tail -30`
Expected: PASS. If a test hardcodes `16`, update it to `17` (search: `rg -n "schema.?version.*16|= 16" internal`). Fix any such assertion to `17`.

- [ ] **Step 7: Commit**

```bash
git add internal/db/schema_version.go internal/db/sqlitestore/schema.sql internal/db/sqlitestore/schema_embeddings_test.go
git commit -m "feat(db): add content_revision column and issue_embeddings table"
```

---

## Task 2: Bump `content_revision` from every title/body writer

**Files:**
- Modify: `internal/db/sqlitestore/queries.go` (add `contentFieldsChanged`; use it in `editIssue`)
- Modify: `internal/db/sqlitestore/queries_edit_atomic.go` (field-change branch)
- Modify: `internal/db/sqlitestore/imports.go` (`updateImportedIssue`)
- Test: `internal/db/sqlitestore/content_revision_test.go` (new)

- [ ] **Step 1: Write the failing test**

Create `internal/db/sqlitestore/content_revision_test.go`:

```go
package sqlitestore

import (
	"context"
	"testing"

	"go.kenn.io/kata/internal/db"
)

func contentRev(ctx context.Context, t *testing.T, d *Store, issueID int64) int64 {
	t.Helper()
	var cr int64
	if err := d.QueryRowContext(ctx,
		`SELECT content_revision FROM issues WHERE id = ?`, issueID).Scan(&cr); err != nil {
		t.Fatalf("read content_revision: %v", err)
	}
	return cr
}

func TestContentRevisionBumpsOnTitleBodyOnly(t *testing.T) {
	ctx := context.Background()
	d := newTestStore(t)
	proj, _ := d.CreateProject(ctx, "spoke-project")
	iss, _, err := d.CreateIssue(ctx, db.CreateIssueParams{
		ProjectID: proj.ID, Title: "first", Body: "b", Author: "tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	base := contentRev(ctx, t, d, iss.ID)

	// Title edit bumps.
	newTitle := "second"
	if _, _, _, err := d.EditIssue(ctx, db.EditIssueParams{IssueID: iss.ID, Title: &newTitle, Actor: "tester"}); err != nil {
		t.Fatal(err)
	}
	afterTitle := contentRev(ctx, t, d, iss.ID)
	if afterTitle != base+1 {
		t.Fatalf("title edit: content_revision = %d, want %d", afterTitle, base+1)
	}

	// Owner edit does NOT bump.
	owner := "alice"
	if _, _, _, err := d.EditIssue(ctx, db.EditIssueParams{IssueID: iss.ID, Owner: &owner, Actor: "tester"}); err != nil {
		t.Fatal(err)
	}
	if got := contentRev(ctx, t, d, iss.ID); got != afterTitle {
		t.Fatalf("owner edit bumped content_revision to %d, want %d", got, afterTitle)
	}

	// Comment does NOT bump.
	if _, _, err := d.CreateComment(ctx, db.CreateCommentParams{IssueID: iss.ID, Body: "c", Author: "tester"}); err != nil {
		t.Fatal(err)
	}
	if got := contentRev(ctx, t, d, iss.ID); got != afterTitle {
		t.Fatalf("comment bumped content_revision to %d, want %d", got, afterTitle)
	}
}
```

Confirm the exact field names of `db.CreateIssueParams` and `db.CreateCommentParams` from `internal/db/params.go` and adjust if needed.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/db/sqlitestore/ -run TestContentRevisionBumpsOnTitleBodyOnly -v`
Expected: FAIL — title edit leaves `content_revision` at base (no bump wired yet).

- [ ] **Step 3: Add the `contentFieldsChanged` helper**

In `internal/db/sqlitestore/queries.go`, add near `issueFieldUpdatePlan`:

```go
// contentFieldsChanged reports whether a title or body edit actually changes
// the embeddable content of issue. It is the precise trigger for bumping
// issues.content_revision; owner, priority, status, comments, links, and
// metadata deliberately do not bump it. See docs/design/semantic-search.md.
func contentFieldsChanged(issue db.Issue, title, body *string) bool {
	if title != nil && *title != issue.Title {
		return true
	}
	if body != nil && *body != issue.Body {
		return true
	}
	return false
}
```

- [ ] **Step 4: Bump in `editIssue`**

In `internal/db/sqlitestore/queries.go`, inside `editIssue`, after the existing `sets = append([]string{`updated_at = ?`}, sets...)` line and before building the query, add the conditional bump:

```go
	sets = append([]string{`updated_at = ?`}, sets...)
	args = append([]any{ts}, args...)
	if contentFieldsChanged(issue, p.Title, p.Body) {
		sets = append(sets, `content_revision = content_revision + 1`)
	}
	args = append(args, p.IssueID)
```

(The `content_revision = content_revision + 1` clause has no placeholder, so it must be appended to `sets` *after* the `ts` prepend but it does not add to `args`.)

- [ ] **Step 5: Bump in `editIssueAtomic`**

In `internal/db/sqlitestore/queries_edit_atomic.go`, inside the `if fieldsChanged {` branch, mirror the change:

```go
	if fieldsChanged {
		sets = append([]string{`updated_at = ?`}, sets...)
		args = append([]any{ts}, args...)
		if contentFieldsChanged(issue, p.Title, p.Body) {
			sets = append(sets, `content_revision = content_revision + 1`)
		}
		args = append(args, p.IssueID)
		q := `UPDATE issues SET ` + joinComma(sets) + ` WHERE id = ?` // #nosec G202
		// ... rest unchanged ...
	}
```

- [ ] **Step 6: Bump in `updateImportedIssue`**

In `internal/db/sqlitestore/imports.go`, replace the fixed `UPDATE issues SET ...` in `updateImportedIssue` so it bumps `content_revision` only when title or body differs from `existing`:

```go
func (d *Store) updateImportedIssue(ctx context.Context, tx *sql.Tx, p db.ImportBatchParams, item db.ImportItem, existing db.Issue, projectName string) (db.Issue, db.Event, error) {
	bump := ""
	if item.Title != existing.Title || item.Body != existing.Body {
		bump = `, content_revision = content_revision + 1`
	}
	_, err := tx.ExecContext(ctx, `UPDATE issues
		SET title = ?, body = ?, status = ?, closed_reason = ?, owner = ?, updated_at = ?, closed_at = ?, priority = ?`+bump+`
		WHERE id = ?`, item.Title, item.Body, item.Status, item.ClosedReason, normalizeOwner(item.Owner), item.UpdatedAt, item.ClosedAt, item.Priority, existing.ID)
	if err != nil {
		return db.Issue{}, db.Event{}, fmt.Errorf("update imported issue: %w", err)
	}
	// ... rest unchanged ...
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `go test ./internal/db/sqlitestore/ -run TestContentRevisionBumpsOnTitleBodyOnly -v`
Expected: PASS.

- [ ] **Step 8: Run import + edit suites for regressions**

Run: `go test ./internal/db/sqlitestore/ 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/db/sqlitestore/queries.go internal/db/sqlitestore/queries_edit_atomic.go internal/db/sqlitestore/imports.go internal/db/sqlitestore/content_revision_test.go
git commit -m "feat(db): bump content_revision from all title/body writers"
```

---

## Task 3: `internal/embedding` — recipe and fingerprint (pure)

**Files:**
- Create: `internal/embedding/recipe.go`
- Test: `internal/embedding/recipe_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/embedding/recipe_test.go`:

```go
package embedding

import (
	"strings"
	"testing"
)

func TestEmbedTextJoinsAndTruncatesOnRuneBoundary(t *testing.T) {
	got := EmbedText("Title", "Body")
	if got != "Title\n\nBody" {
		t.Fatalf("join: got %q", got)
	}
	long := strings.Repeat("é", maxEmbedChars) // 2 bytes each; forces boundary care
	out := EmbedText(long, "")
	if len([]rune(out)) > maxEmbedChars {
		t.Fatalf("truncation exceeded rune cap: %d", len([]rune(out)))
	}
	if !strings.HasPrefix(long+"\n\n", out) && !strings.HasPrefix(out, "é") {
		t.Fatalf("truncation produced invalid prefix")
	}
}

func TestFingerprintIsStableAndComponentSensitive(t *testing.T) {
	base := Fingerprint("nomic-embed-text", 768, "")
	if len(base) != 64 {
		t.Fatalf("fingerprint length = %d, want 64", len(base))
	}
	if base == Fingerprint("other-model", 768, "") {
		t.Fatal("model change did not alter fingerprint")
	}
	if base == Fingerprint("nomic-embed-text", 1024, "") {
		t.Fatal("dims change did not alter fingerprint")
	}
	if base == Fingerprint("nomic-embed-text", 768, "salt") {
		t.Fatal("salt change did not alter fingerprint")
	}
	if base != Fingerprint("nomic-embed-text", 768, "") {
		t.Fatal("fingerprint not deterministic")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/embedding/ -run 'TestEmbedText|TestFingerprint' -v`
Expected: FAIL — package/functions do not exist.

- [ ] **Step 3: Implement recipe and fingerprint**

Create `internal/embedding/recipe.go`:

```go
// Package embedding produces vector embeddings of issue text via an
// OpenAI-compatible HTTP endpoint. It is storage-free: it imports neither
// internal/db nor internal/daemon and operates on plain strings.
package embedding

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// RecipeVersion is part of the fingerprint. Bump it when EmbedText changes,
// so every stored embedding is recomputed against the new text recipe.
const RecipeVersion = 1

// maxEmbedChars caps the runes sent to the embedder. Most embedding models
// truncate long inputs anyway; capping keeps batch payloads bounded.
const maxEmbedChars = 8000

// EmbedText is the v1 recipe: title and body joined, truncated on a rune
// boundary. Comments are intentionally excluded (see the design note).
func EmbedText(title, body string) string {
	s := title + "\n\n" + body
	r := []rune(s)
	if len(r) > maxEmbedChars {
		r = r[:maxEmbedChars]
	}
	return string(r)
}

// Fingerprint identifies the (model, dims, recipe, salt) tuple a vector was
// produced under. A change in any component invalidates stored vectors. The
// endpoint URL is deliberately excluded so moving a port or host does not
// force a re-embed. salt is the operator's lever for "same model name,
// different weights".
func Fingerprint(model string, dims int, salt string) string {
	h := sha256.New()
	fmt.Fprintf(h, "v%d\x00%s\x00%d\x00%s", RecipeVersion, model, dims, salt)
	return hex.EncodeToString(h.Sum(nil))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/embedding/ -run 'TestEmbedText|TestFingerprint' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/embedding/recipe.go internal/embedding/recipe_test.go
git commit -m "feat(embedding): add text recipe and fingerprint"
```

---

## Task 4: `internal/embedding` — HTTP client

**Files:**
- Create: `internal/embedding/client.go`
- Test: `internal/embedding/client_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/embedding/client_test.go`:

```go
package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newFakeServer(t *testing.T, status int, body string, retryAfter string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func TestEmbedNormalizesVectors(t *testing.T) {
	srv := newFakeServer(t, 200, `{"data":[{"embedding":[3,4]}]}`, "")
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL, Model: "m", Dims: 2})
	if err != nil {
		t.Fatal(err)
	}
	vecs, err := c.Embed(context.Background(), []string{"hello"})
	if err != nil {
		t.Fatal(err)
	}
	// [3,4] normalized is [0.6,0.8].
	if math.Abs(float64(vecs[0][0])-0.6) > 1e-6 || math.Abs(float64(vecs[0][1])-0.8) > 1e-6 {
		t.Fatalf("not normalized: %v", vecs[0])
	}
}

func TestEmbedDimsMismatchIsDefinitive(t *testing.T) {
	srv := newFakeServer(t, 200, `{"data":[{"embedding":[1,2,3]}]}`, "")
	defer srv.Close()
	c, _ := New(Config{BaseURL: srv.URL, Model: "m", Dims: 2})
	_, err := c.Embed(context.Background(), []string{"x"})
	if err == nil {
		t.Fatal("expected dims-mismatch error")
	}
}

func TestEmbed401IsDefinitive(t *testing.T) {
	srv := newFakeServer(t, 401, `{"error":"bad key"}`, "")
	defer srv.Close()
	c, _ := New(Config{BaseURL: srv.URL, Model: "m", Dims: 2})
	_, err := c.Embed(context.Background(), []string{"x"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || !apiErr.Definitive() {
		t.Fatalf("want definitive APIError, got %v", err)
	}
}

func TestEmbed429CarriesRetryAfter(t *testing.T) {
	srv := newFakeServer(t, 429, `{}`, "7")
	defer srv.Close()
	c, _ := New(Config{BaseURL: srv.URL, Model: "m", Dims: 2})
	_, err := c.Embed(context.Background(), []string{"x"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want APIError, got %v", err)
	}
	if apiErr.Definitive() {
		t.Fatal("429 must not be definitive")
	}
	if apiErr.RetryAfter != 7*time.Second {
		t.Fatalf("RetryAfter = %v, want 7s", apiErr.RetryAfter)
	}
}

func TestEmbedKeyOnlyToConfiguredOrigin(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"embedding": []float32{1, 0}}}})
	}))
	defer srv.Close()
	c, _ := New(Config{BaseURL: srv.URL, Model: "m", Dims: 2, APIKey: "secret"})
	if _, err := c.Embed(context.Background(), []string{"x"}); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer secret" {
		t.Fatalf("auth header = %q", gotAuth)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/embedding/ -run TestEmbed -v`
Expected: FAIL — `New`/`Embed`/`APIError` undefined.

- [ ] **Step 3: Implement the client**

Create `internal/embedding/client.go`:

```go
package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.kenn.io/kata/internal/config"
)

// Config configures an embedding Client. BaseURL and Model are required by the
// caller (daemon config validation enforces this). Dims defaults to 768.
type Config struct {
	BaseURL             string
	Model               string
	APIKey              string
	Salt                string
	Dims                int
	BatchSize           int
	Timeout             time.Duration
	TrustPrivateNetwork bool
}

// Client calls an OpenAI-compatible /embeddings endpoint.
type Client struct {
	http      *http.Client
	baseURL   string
	model     string
	salt      string
	dims      int
	batchSize int
}

const (
	defaultDims      = 768
	defaultBatchSize = 64
	defaultTimeout   = 30 * time.Second
)

// New builds a Client with an origin-pinned HTTP transport. The API key is
// attached only to the configured origin (see internal/config/bearer.go);
// cross-origin redirects are refused.
func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" || strings.TrimSpace(cfg.Model) == "" {
		return nil, fmt.Errorf("embedding: base_url and model are required")
	}
	dims := cfg.Dims
	if dims <= 0 {
		dims = defaultDims
	}
	batch := cfg.BatchSize
	if batch <= 0 {
		batch = defaultBatchSize
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	hc := &http.Client{Timeout: timeout}
	if cfg.APIKey != "" {
		if err := config.ConfigureBearerClientWithTrust(hc, cfg.BaseURL, cfg.APIKey, cfg.TrustPrivateNetwork); err != nil {
			return nil, fmt.Errorf("embedding: configure client: %w", err)
		}
	}
	return &Client{
		http:      hc,
		baseURL:   strings.TrimRight(cfg.BaseURL, "/"),
		model:     cfg.Model,
		salt:      cfg.Salt,
		dims:      dims,
		batchSize: batch,
	}, nil
}

// Dims returns the configured/expected vector dimensionality.
func (c *Client) Dims() int { return c.dims }

// Fingerprint identifies the model/dims/recipe/salt of vectors this client
// produces.
func (c *Client) Fingerprint() string { return Fingerprint(c.model, c.dims, c.salt) }

// BatchSize is the maximum number of inputs per request.
func (c *Client) BatchSize() int { return c.batchSize }

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// APIError is a non-2xx response from the embedding endpoint.
type APIError struct {
	StatusCode int
	RetryAfter time.Duration
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("embedding endpoint returned %d: %s", e.StatusCode, e.Body)
}

// Definitive reports whether retrying is pointless without operator action.
func (e *APIError) Definitive() bool {
	switch e.StatusCode {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return true
	}
	return false
}

// Embed returns one L2-normalized vector per input, preserving order. Inputs
// are sent in batches of at most BatchSize.
func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += c.batchSize {
		end := start + c.batchSize
		if end > len(texts) {
			end = len(texts)
		}
		vecs, err := c.embedBatch(ctx, texts[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, vecs...)
	}
	return out, nil
}

func (c *Client) embedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(embedRequest{Model: c.model, Input: texts})
	if err != nil {
		return nil, fmt.Errorf("embedding: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			Body:       string(rb),
		}
	}
	var er embedResponse
	if err := json.Unmarshal(rb, &er); err != nil {
		return nil, fmt.Errorf("embedding: decode response: %w", err)
	}
	if len(er.Data) != len(texts) {
		return nil, fmt.Errorf("embedding: got %d vectors for %d inputs", len(er.Data), len(texts))
	}
	vecs := make([][]float32, len(er.Data))
	for i, d := range er.Data {
		if len(d.Embedding) != c.dims {
			return nil, fmt.Errorf("embedding: vector dims %d != configured %d", len(d.Embedding), c.dims)
		}
		vecs[i] = normalize(d.Embedding)
	}
	return vecs, nil
}

func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(strings.TrimSpace(h)); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

func normalize(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return v
	}
	inv := float32(1 / math.Sqrt(sum))
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * inv
	}
	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/embedding/ -v`
Expected: PASS (all client + recipe tests).

- [ ] **Step 5: Commit**

```bash
git add internal/embedding/client.go internal/embedding/client_test.go
git commit -m "feat(embedding): add OpenAI-compatible client with error classification"
```

---

## Task 5: Config — `[search.embeddings]` section

**Files:**
- Modify: `internal/config/daemon_config.go`
- Test: `internal/config/daemon_config_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/config/daemon_config_test.go`:

```go
func TestSearchEmbeddingsConfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KATA_HOME", dir)
	write := func(body string) {
		if err := os.WriteFile(filepath.Join(dir, "config.toml"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// Valid: base_url + model present.
	write(`
[search.embeddings]
base_url = "http://localhost:11434/v1"
model = "nomic-embed-text"
`)
	cfg, err := ReadDaemonConfig()
	if err != nil {
		t.Fatalf("valid config: %v", err)
	}
	if !cfg.Search.Embeddings.Enabled() {
		t.Fatal("expected embeddings enabled")
	}

	// Invalid: base_url without model.
	write(`
[search.embeddings]
base_url = "http://localhost:11434/v1"
`)
	if _, err := ReadDaemonConfig(); err == nil {
		t.Fatal("expected error for base_url without model")
	}

	// Invalid: api_key and api_key_env both set.
	write(`
[search.embeddings]
base_url = "http://localhost:11434/v1"
model = "m"
api_key = "x"
api_key_env = "Y"
`)
	if _, err := ReadDaemonConfig(); err == nil {
		t.Fatal("expected error for api_key + api_key_env")
	}
}
```

Confirm `ReadDaemonConfig` reads from `KATA_HOME`; if the path helper differs, match the existing config tests in this file.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestSearchEmbeddingsConfig -v`
Expected: FAIL — `cfg.Search` undefined.

- [ ] **Step 3: Add the config structs**

In `internal/config/daemon_config.go`, add to `DaemonConfig`:

```go
type DaemonConfig struct {
	Listen       string                `toml:"listen"`
	ActiveDaemon string                `toml:"active_daemon"`
	Daemons      []CatalogDaemonConfig `toml:"daemon"`
	TUI          TUIConfig             `toml:"tui"`
	Close        CloseConfig           `toml:"close"`
	Auth         AuthConfig            `toml:"auth"`
	Storage      StorageConfig         `toml:"storage"`
	Search       SearchConfig          `toml:"search"`
}

type SearchConfig struct {
	Embeddings EmbeddingsConfig `toml:"embeddings"`
}

type EmbeddingsConfig struct {
	BaseURL             string `toml:"base_url"`
	Model               string `toml:"model"`
	APIKey              string `toml:"api_key"`
	APIKeyEnv           string `toml:"api_key_env"`
	FingerprintSalt     string `toml:"fingerprint_salt"`
	Dims                int    `toml:"dims"`
	BatchSize           int    `toml:"batch_size"`
	TimeoutSeconds      int    `toml:"timeout_seconds"`
	TrustPrivateNetwork bool   `toml:"trust_private_network"`
}

// Enabled reports whether semantic search is configured.
func (e EmbeddingsConfig) Enabled() bool {
	return strings.TrimSpace(e.BaseURL) != "" && strings.TrimSpace(e.Model) != ""
}

// ResolvedAPIKey returns the literal api_key, or the value of api_key_env.
func (e EmbeddingsConfig) ResolvedAPIKey() string {
	if e.APIKey != "" {
		return e.APIKey
	}
	if e.APIKeyEnv != "" {
		return os.Getenv(strings.TrimSpace(e.APIKeyEnv))
	}
	return ""
}
```

- [ ] **Step 4: Add validation**

In `ReadDaemonConfig` (after TOML decode, alongside the other validation calls), add a call to a new `validateEmbeddings`:

```go
func validateEmbeddings(e EmbeddingsConfig) error {
	hasBase := strings.TrimSpace(e.BaseURL) != ""
	hasModel := strings.TrimSpace(e.Model) != ""
	if hasBase != hasModel {
		return fmt.Errorf("[search.embeddings]: base_url and model must both be set or both omitted")
	}
	if e.APIKey != "" && e.APIKeyEnv != "" {
		return fmt.Errorf("[search.embeddings]: api_key and api_key_env are mutually exclusive")
	}
	if e.Dims < 0 || e.BatchSize < 0 || e.TimeoutSeconds < 0 {
		return fmt.Errorf("[search.embeddings]: dims, batch_size, timeout_seconds must be non-negative")
	}
	return nil
}
```

Wire it in `ReadDaemonConfig` next to the existing validations:

```go
	if err := validateEmbeddings(cfg.Search.Embeddings); err != nil {
		return nil, err
	}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestSearchEmbeddingsConfig -v`
Expected: PASS.

- [ ] **Step 6: Run full config suite**

Run: `go test ./internal/config/ 2>&1 | tail -15`
Expected: PASS (the unknown-key validator now accepts `[search.embeddings]`).

- [ ] **Step 7: Commit**

```bash
git add internal/config/daemon_config.go internal/config/daemon_config_test.go
git commit -m "feat(config): add [search.embeddings] section with validation"
```

---

## Task 6: Storage interface + sqlitestore — embedding CRUD

**Files:**
- Create: `internal/db/embedding_types.go`
- Modify: `internal/db/storage.go` (interface)
- Modify: `internal/db/pgstore/stubs_gen.go` (stubs)
- Create: `internal/db/sqlitestore/queries_embeddings.go`
- Test: `internal/db/sqlitestore/queries_embeddings_test.go`

- [ ] **Step 1: Add the shared types**

Create `internal/db/embedding_types.go`:

```go
package db

// IssueEmbedding is one stored vector for an issue.
type IssueEmbedding struct {
	IssueID                 int64
	EmbeddedContentRevision int64
	Fingerprint             string
	Dims                    int
	Vector                  []float32 // L2-normalized
}

// EmbedTarget is an issue whose embedding is missing or stale for the active
// fingerprint, carrying the text the reconciler must embed.
type EmbedTarget struct {
	IssueID         int64
	ContentRevision int64
	Title           string
	Body            string
}
```

- [ ] **Step 2: Write the failing test**

Create `internal/db/sqlitestore/queries_embeddings_test.go`:

```go
package sqlitestore

import (
	"context"
	"testing"

	"go.kenn.io/kata/internal/db"
)

func mkIssue(ctx context.Context, t *testing.T, d *Store, projID int64, title string) db.Issue {
	t.Helper()
	iss, _, err := d.CreateIssue(ctx, db.CreateIssueParams{ProjectID: projID, Title: title, Body: "body", Author: "tester"})
	if err != nil {
		t.Fatal(err)
	}
	return iss
}

func TestListEmbedTargetsAndUpsert(t *testing.T) {
	ctx := context.Background()
	d := newTestStore(t)
	proj, _ := d.CreateProject(ctx, "spoke-project")
	iss := mkIssue(ctx, t, d, proj.ID, "needs embedding")
	fp := "a" + repeat63 // 64-char fingerprint helper below

	// Initially: the issue is a target (no row yet).
	targets, err := d.ListEmbedTargets(ctx, fp, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].IssueID != iss.ID {
		t.Fatalf("expected issue as target, got %#v", targets)
	}

	// Upsert an embedding at the issue's current content_revision.
	if err := d.UpsertIssueEmbedding(ctx, db.IssueEmbedding{
		IssueID: iss.ID, EmbeddedContentRevision: targets[0].ContentRevision,
		Fingerprint: fp, Dims: 2, Vector: []float32{1, 0},
	}); err != nil {
		t.Fatal(err)
	}

	// Now it is not a target.
	targets, _ = d.ListEmbedTargets(ctx, fp, 10)
	if len(targets) != 0 {
		t.Fatalf("expected no targets after upsert, got %d", len(targets))
	}

	// Editing the title makes it a target again (content_revision moved).
	nt := "edited"
	if _, _, _, err := d.EditIssue(ctx, db.EditIssueParams{IssueID: iss.ID, Title: &nt, Actor: "tester"}); err != nil {
		t.Fatal(err)
	}
	targets, _ = d.ListEmbedTargets(ctx, fp, 10)
	if len(targets) != 1 {
		t.Fatalf("expected stale issue as target, got %d", len(targets))
	}

	// A different fingerprint sees the issue as a target (model swap).
	targets, _ = d.ListEmbedTargets(ctx, "b"+repeat63, 10)
	if len(targets) != 1 {
		t.Fatalf("fingerprint swap should re-target, got %d", len(targets))
	}
}

const repeat63 = "000000000000000000000000000000000000000000000000000000000000000"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/db/sqlitestore/ -run TestListEmbedTargetsAndUpsert -v`
Expected: FAIL — methods undefined.

- [ ] **Step 4: Add the interface methods**

In `internal/db/storage.go`, in the `// search` group, add (note: `SearchVector` is intentionally deferred to Task 7 so the interface only gains it when sqlitestore also implements it — keeping every task's build green):

```go
	// embeddings (semantic search)
	UpsertIssueEmbedding(ctx context.Context, e IssueEmbedding) error
	ListEmbedTargets(ctx context.Context, fingerprint string, limit int) ([]EmbedTarget, error)
	EmbeddingStats(ctx context.Context, projectID int64, fingerprint string) (count int64, maxUpdatedAt string, err error)
```

- [ ] **Step 5: Implement upsert + list in sqlitestore**

Create `internal/db/sqlitestore/queries_embeddings.go` (SearchVector is added in Task 7):

```go
package sqlitestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"go.kenn.io/kata/internal/db"
)

// vectorToBytes serializes float32s little-endian.
func vectorToBytes(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

func bytesToVector(b []byte) []float32 {
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return v
}

// UpsertIssueEmbedding inserts or replaces the embedding row for an issue.
func (d *Store) UpsertIssueEmbedding(ctx context.Context, e db.IssueEmbedding) error {
	return d.RetryTransient(ctx, func() error {
		_, err := d.ExecContext(ctx, `
			INSERT INTO issue_embeddings
			  (issue_id, embedded_content_revision, embed_fingerprint, dims, vector_bytes, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(issue_id) DO UPDATE SET
			  embedded_content_revision = excluded.embedded_content_revision,
			  embed_fingerprint = excluded.embed_fingerprint,
			  dims = excluded.dims,
			  vector_bytes = excluded.vector_bytes,
			  updated_at = excluded.updated_at`,
			e.IssueID, e.EmbeddedContentRevision, e.Fingerprint, e.Dims,
			vectorToBytes(e.Vector), nowTimestamp())
		if err != nil {
			return fmt.Errorf("upsert issue embedding: %w", err)
		}
		// Invalidate the in-memory cache for this issue's project/fingerprint.
		d.vectorCache.invalidate(e.Fingerprint)
		return nil
	})
}

// ListEmbedTargets returns issues missing an embedding, embedded under a
// different fingerprint, or whose content_revision has moved since embedding.
// Soft-deleted issues are excluded.
func (d *Store) ListEmbedTargets(ctx context.Context, fingerprint string, limit int) ([]db.EmbedTarget, error) {
	if limit <= 0 {
		limit = 64
	}
	rows, err := d.QueryContext(ctx, `
		SELECT i.id, i.content_revision, i.title, i.body
		FROM issues i
		LEFT JOIN issue_embeddings e ON e.issue_id = i.id
		WHERE i.deleted_at IS NULL
		  AND (e.issue_id IS NULL
		       OR e.embed_fingerprint != ?
		       OR e.embedded_content_revision != i.content_revision)
		ORDER BY i.id ASC
		LIMIT ?`, fingerprint, limit)
	if err != nil {
		return nil, fmt.Errorf("list embed targets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []db.EmbedTarget
	for rows.Next() {
		var t db.EmbedTarget
		if err := rows.Scan(&t.IssueID, &t.ContentRevision, &t.Title, &t.Body); err != nil {
			return nil, fmt.Errorf("scan embed target: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// EmbeddingStats returns the count and max updated_at of embedding rows for a
// project at the active fingerprint — the per-query cache freshness probe.
func (d *Store) EmbeddingStats(ctx context.Context, projectID int64, fingerprint string) (int64, string, error) {
	var count int64
	var maxUpdated *string
	err := d.QueryRowContext(ctx, `
		SELECT count(*), max(e.updated_at)
		FROM issue_embeddings e
		JOIN issues i ON i.id = e.issue_id
		WHERE i.project_id = ? AND e.embed_fingerprint = ?`,
		projectID, fingerprint).Scan(&count, &maxUpdated)
	if err != nil {
		return 0, "", fmt.Errorf("embedding stats: %w", err)
	}
	if maxUpdated == nil {
		return count, "", nil
	}
	return count, *maxUpdated, nil
}
```

- [ ] **Step 6: Add the cache field and stub to `Store`**

The cache type is implemented in Task 7. For now, add the field to the `Store` struct (`internal/db/sqlitestore/store.go`) and a no-op `invalidate` so this task compiles:

```go
// in store.go, Store struct:
	vectorCache *vectorCache
```

Create `internal/db/sqlitestore/vector_cache.go` with the minimal type (filled in Task 7):

```go
package sqlitestore

import "sync"

type vectorCache struct {
	mu sync.Mutex
	// entries keyed by "<projectID>:<fingerprint>"; populated in Task 7.
}

func newVectorCache() *vectorCache { return &vectorCache{} }

func (c *vectorCache) invalidate(fingerprint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Task 7 fills this in; for now there is nothing cached to drop.
}
```

Initialize it in `Open` (where the `Store` value is constructed in `store.go`): set `vectorCache: newVectorCache()`.

- [ ] **Step 7: Stub the pgstore methods**

In `internal/db/pgstore/stubs_gen.go`, add (matching the file's existing stub style returning `ErrNotImplementedPhase3` or equivalent):

```go
func (s *Store) UpsertIssueEmbedding(_ context.Context, _ db.IssueEmbedding) error {
	return errNotImplemented("UpsertIssueEmbedding")
}
func (s *Store) ListEmbedTargets(_ context.Context, _ string, _ int) ([]db.EmbedTarget, error) {
	return nil, errNotImplemented("ListEmbedTargets")
}
func (s *Store) EmbeddingStats(_ context.Context, _ int64, _ string) (int64, string, error) {
	return 0, "", errNotImplemented("EmbeddingStats")
}
```

(`SearchVector`'s pgstore stub is added in Task 7, together with the interface line and the sqlitestore implementation.)

Match the actual stub helper name used in that file (check the top of `stubs_gen.go` for the existing `errNotImplemented`/`ErrNotImplementedPhase3` form).

- [ ] **Step 8: Run test to verify it passes**

Run: `go test ./internal/db/sqlitestore/ -run TestListEmbedTargetsAndUpsert -v`
Expected: PASS.

- [ ] **Step 9: Verify the whole module still builds (interface satisfied)**

Run: `go build ./... && go vet ./internal/db/...`
Expected: no errors (both backends satisfy `db.Storage`).

- [ ] **Step 10: Commit**

```bash
git add internal/db/embedding_types.go internal/db/storage.go internal/db/pgstore/stubs_gen.go internal/db/sqlitestore/queries_embeddings.go internal/db/sqlitestore/vector_cache.go internal/db/sqlitestore/store.go internal/db/sqlitestore/queries_embeddings_test.go
git commit -m "feat(db): add embedding upsert, target listing, and stats"
```

---

## Task 7: Storage — `SearchVector` with fingerprint-scoped cache

**Files:**
- Modify: `internal/db/storage.go` (add `SearchVector` to the interface)
- Modify: `internal/db/pgstore/stubs_gen.go` (stub `SearchVector`)
- Modify: `internal/db/sqlitestore/vector_cache.go`
- Modify: `internal/db/sqlitestore/queries_embeddings.go` (add `SearchVector`)
- Test: `internal/db/sqlitestore/queries_embeddings_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/db/sqlitestore/queries_embeddings_test.go`:

```go
func TestSearchVectorRanksAndRespectsVisibility(t *testing.T) {
	ctx := context.Background()
	d := newTestStore(t)
	proj, _ := d.CreateProject(ctx, "spoke-project")
	fp := "a" + repeat63

	near := mkIssue(ctx, t, d, proj.ID, "near")
	far := mkIssue(ctx, t, d, proj.ID, "far")
	embed := func(iss db.Issue, v []float32) {
		cr := contentRev(ctx, t, d, iss.ID)
		if err := d.UpsertIssueEmbedding(ctx, db.IssueEmbedding{
			IssueID: iss.ID, EmbeddedContentRevision: cr, Fingerprint: fp, Dims: 2, Vector: v,
		}); err != nil {
			t.Fatal(err)
		}
	}
	embed(near, []float32{1, 0})
	embed(far, []float32{0, 1})

	// Query close to "near".
	hits, err := d.SearchVector(ctx, proj.ID, []float32{1, 0}, fp, 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].Issue.ID != near.ID {
		t.Fatalf("ranking wrong: %#v", hits)
	}
	if hits[0].Score <= hits[1].Score {
		t.Fatalf("expected descending similarity: %v %v", hits[0].Score, hits[1].Score)
	}
	for _, h := range hits {
		if len(h.MatchedIn) != 1 || h.MatchedIn[0] != "semantic" {
			t.Fatalf("matched_in = %v", h.MatchedIn)
		}
	}

	// Soft-delete "near": it must drop out (visibility resolved live).
	if _, _, _, err := d.SoftDeleteIssue(ctx, near.ID, "tester"); err != nil {
		t.Fatal(err)
	}
	hits, _ = d.SearchVector(ctx, proj.ID, []float32{1, 0}, fp, 10, false)
	if len(hits) != 1 || hits[0].Issue.ID != far.ID {
		t.Fatalf("soft-deleted issue leaked: %#v", hits)
	}

	// A wrong-fingerprint query returns nothing (no cross-model compare).
	hits, _ = d.SearchVector(ctx, proj.ID, []float32{1, 0}, "b"+repeat63, 10, false)
	if len(hits) != 0 {
		t.Fatalf("fingerprint filter failed: %#v", hits)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/db/sqlitestore/ -run TestSearchVectorRanks -v`
Expected: FAIL — `SearchVector` undefined.

- [ ] **Step 3: Implement the cache**

Replace `internal/db/sqlitestore/vector_cache.go`:

```go
package sqlitestore

import (
	"fmt"
	"sync"
)

type cachedVec struct {
	issueID int64
	vec     []float32
}

type cacheEntry struct {
	count      int64
	maxUpdated string
	vecs       []cachedVec
}

// vectorCache holds normalized vectors per (projectID, fingerprint). It never
// holds visibility or row data: SearchVector resolves candidate ids against
// the live issues table on every call. Freshness is validated per query with
// a (count, maxUpdated) probe, so a model swap (new fingerprint) lands in a
// fresh entry and the old one is abandoned.
type vectorCache struct {
	mu      sync.Mutex
	entries map[string]*cacheEntry
}

func newVectorCache() *vectorCache {
	return &vectorCache{entries: map[string]*cacheEntry{}}
}

func cacheKey(projectID int64, fingerprint string) string {
	return fmt.Sprintf("%d:%s", projectID, fingerprint)
}

func (c *vectorCache) invalidate(fingerprint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.entries {
		// Key is "<projectID>:<fingerprint>"; drop any entry for this fp.
		if len(k) > len(fingerprint) && k[len(k)-len(fingerprint):] == fingerprint {
			delete(c.entries, k)
		}
	}
}

// get returns cached vectors when the probe matches; ok is false otherwise.
func (c *vectorCache) get(projectID int64, fingerprint string, count int64, maxUpdated string) ([]cachedVec, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[cacheKey(projectID, fingerprint)]
	if !ok || e.count != count || e.maxUpdated != maxUpdated {
		return nil, false
	}
	return e.vecs, true
}

func (c *vectorCache) put(projectID int64, fingerprint string, count int64, maxUpdated string, vecs []cachedVec) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[cacheKey(projectID, fingerprint)] = &cacheEntry{count: count, maxUpdated: maxUpdated, vecs: vecs}
}
```

- [ ] **Step 4: Add `SearchVector` to the interface and pgstore stub**

In `internal/db/storage.go`, add to the embeddings group:

```go
	SearchVector(ctx context.Context, projectID int64, queryVec []float32, fingerprint string, k int, includeDeleted bool) ([]SearchCandidate, error)
```

In `internal/db/pgstore/stubs_gen.go`, add (matching the file's stub style):

```go
func (s *Store) SearchVector(_ context.Context, _ int64, _ []float32, _ string, _ int, _ bool) ([]db.SearchCandidate, error) {
	return nil, errNotImplemented("SearchVector")
}
```

- [ ] **Step 5: Implement `SearchVector` in sqlitestore**

Add to `internal/db/sqlitestore/queries_embeddings.go`:

```go
// SearchVector returns up to k issues ranked by cosine similarity to queryVec,
// scoped to projectID and the active fingerprint. Vectors are cached per
// (project, fingerprint); the cache supplies candidates and similarities only.
// Visibility and row data always come from a live join against issues, so
// soft-delete/restore/purge can never surface a wrong result.
func (d *Store) SearchVector(ctx context.Context, projectID int64, queryVec []float32, fingerprint string, k int, includeDeleted bool) ([]db.SearchCandidate, error) {
	if k <= 0 {
		k = 20
	}
	count, maxUpdated, err := d.EmbeddingStats(ctx, projectID, fingerprint)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	vecs, ok := d.vectorCache.get(projectID, fingerprint, count, maxUpdated)
	if !ok {
		vecs, err = d.loadVectors(ctx, projectID, fingerprint)
		if err != nil {
			return nil, err
		}
		d.vectorCache.put(projectID, fingerprint, count, maxUpdated, vecs)
	}

	// Rank all candidates by dot product (vectors are L2-normalized → cosine).
	ranked := make([]scoredVec, 0, len(vecs))
	for _, cv := range vecs {
		ranked = append(ranked, scoredVec{issueID: cv.issueID, score: dot(queryVec, cv.vec)})
	}
	sortScoredVecDesc(ranked)

	// Walk ranked candidates, resolving live rows in batches until k found.
	out := make([]db.SearchCandidate, 0, k)
	for _, s := range ranked {
		if len(out) >= k {
			break
		}
		iss, err := d.liveIssue(ctx, s.issueID, includeDeleted)
		if err != nil {
			return nil, err
		}
		if iss == nil {
			continue // purged or (when !includeDeleted) soft-deleted
		}
		out = append(out, db.SearchCandidate{Issue: *iss, Score: s.score, MatchedIn: []string{"semantic"}})
	}
	return out, nil
}

func (d *Store) loadVectors(ctx context.Context, projectID int64, fingerprint string) ([]cachedVec, error) {
	rows, err := d.QueryContext(ctx, `
		SELECT e.issue_id, e.vector_bytes
		FROM issue_embeddings e
		JOIN issues i ON i.id = e.issue_id
		WHERE i.project_id = ? AND e.embed_fingerprint = ?`,
		projectID, fingerprint)
	if err != nil {
		return nil, fmt.Errorf("load vectors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []cachedVec
	for rows.Next() {
		var id int64
		var b []byte
		if err := rows.Scan(&id, &b); err != nil {
			return nil, fmt.Errorf("scan vector: %w", err)
		}
		out = append(out, cachedVec{issueID: id, vec: bytesToVector(b)})
	}
	return out, rows.Err()
}

// liveIssue returns the issue if visible, or nil if absent/soft-deleted (when
// includeDeleted is false). Uses the shared issueSelect for full row data.
func (d *Store) liveIssue(ctx context.Context, id int64, includeDeleted bool) (*db.Issue, error) {
	where := ` WHERE i.id = ?`
	if !includeDeleted {
		where += ` AND i.deleted_at IS NULL`
	}
	iss, err := scanIssue(d.QueryRowContext(ctx, issueSelect+where, id))
	if errorsIsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &iss, nil
}

func dot(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}
```

Add the supporting type and helpers in the same file (add `"sort"` and `"errors"` to the import block):

```go
type scoredVec struct {
	issueID int64
	score   float64
}

func sortScoredVecDesc(xs []scoredVec) {
	sort.SliceStable(xs, func(i, j int) bool {
		if xs[i].score != xs[j].score {
			return xs[i].score > xs[j].score
		}
		return xs[i].issueID < xs[j].issueID
	})
}

// scanIssue returns db.ErrNotFound when a row is absent; liveIssue treats that
// as "not visible" rather than an error.
func errorsIsNotFound(err error) bool { return errors.Is(err, db.ErrNotFound) }
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./internal/db/sqlitestore/ -run TestSearchVectorRanks -v`
Expected: PASS.

- [ ] **Step 7: Run the full sqlitestore suite and build both backends**

Run: `go test ./internal/db/sqlitestore/ 2>&1 | tail -15 && go build ./...`
Expected: PASS; build clean (both backends satisfy `db.Storage` now that `SearchVector` exists everywhere).

- [ ] **Step 8: Commit**

```bash
git add internal/db/storage.go internal/db/pgstore/stubs_gen.go internal/db/sqlitestore/vector_cache.go internal/db/sqlitestore/queries_embeddings.go internal/db/sqlitestore/queries_embeddings_test.go
git commit -m "feat(db): add brute-force SearchVector with fingerprint-scoped cache"
```

---

## Task 8: RRF merge and mode resolution (pure)

**Files:**
- Create: `internal/daemon/rrf.go`
- Test: `internal/daemon/rrf_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/daemon/rrf_test.go`:

```go
package daemon

import (
	"testing"

	"go.kenn.io/kata/internal/db"
)

func cand(id int64, score float64, matched ...string) db.SearchCandidate {
	return db.SearchCandidate{Issue: db.Issue{ID: id}, Score: score, MatchedIn: matched}
}

func TestMergeRRFCombinesAndDedupes(t *testing.T) {
	lex := []db.SearchCandidate{cand(1, 5, "title"), cand(2, 4, "body")}
	vec := []db.SearchCandidate{cand(2, 0.9, "semantic"), cand(3, 0.8, "semantic")}
	merged := mergeRRF(lex, vec, 10)

	// Issue 2 appears in both legs → ranks first.
	if merged[0].Issue.ID != 2 {
		t.Fatalf("expected issue 2 first, got %d", merged[0].Issue.ID)
	}
	// matched_in for issue 2 is the union.
	if !contains(merged[0].MatchedIn, "body") || !contains(merged[0].MatchedIn, "semantic") {
		t.Fatalf("matched_in not unioned: %v", merged[0].MatchedIn)
	}
	if len(merged) != 3 {
		t.Fatalf("expected 3 unique issues, got %d", len(merged))
	}
}

func TestMergeRRFEmptyLegs(t *testing.T) {
	if got := mergeRRF(nil, nil, 10); len(got) != 0 {
		t.Fatalf("empty legs should yield empty, got %d", len(got))
	}
	lex := []db.SearchCandidate{cand(1, 5, "title")}
	if got := mergeRRF(lex, nil, 10); len(got) != 1 || got[0].Issue.ID != 1 {
		t.Fatalf("lexical-only passthrough failed: %#v", got)
	}
}

func TestResolveMode(t *testing.T) {
	cases := []struct {
		req        string
		configured bool
		want       searchMode
		wantErr    bool
	}{
		{"", false, modeLexical, false},
		{"", true, modeHybrid, false},
		{"auto", true, modeHybrid, false},
		{"lexical", false, modeLexical, false},
		{"hybrid", false, modeLexical, true},  // 400
		{"semantic", false, modeLexical, true}, // 400
		{"hybrid", true, modeHybrid, false},
		{"bogus", true, modeLexical, true},
	}
	for _, tc := range cases {
		got, err := resolveMode(tc.req, tc.configured)
		if (err != nil) != tc.wantErr {
			t.Fatalf("resolveMode(%q,%v) err=%v wantErr=%v", tc.req, tc.configured, err, tc.wantErr)
		}
		if err == nil && got != tc.want {
			t.Fatalf("resolveMode(%q,%v)=%v want %v", tc.req, tc.configured, got, tc.want)
		}
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/daemon/ -run 'TestMergeRRF|TestResolveMode' -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement RRF and mode resolution**

Create `internal/daemon/rrf.go`:

```go
package daemon

import (
	"fmt"
	"sort"

	"go.kenn.io/kata/internal/db"
)

type searchMode string

const (
	modeLexical  searchMode = "lexical"
	modeHybrid   searchMode = "hybrid"
	modeSemantic searchMode = "semantic"
)

const rrfK = 60

// resolveMode maps a requested mode string and whether embeddings are
// configured to the effective mode. An explicit hybrid/semantic request that
// cannot be served (unconfigured) returns an error so the handler can reply
// 400; "auto"/"" silently resolves to hybrid-when-configured, else lexical.
func resolveMode(requested string, configured bool) (searchMode, error) {
	switch requested {
	case "", "auto":
		if configured {
			return modeHybrid, nil
		}
		return modeLexical, nil
	case "lexical":
		return modeLexical, nil
	case "hybrid", "semantic":
		if !configured {
			return modeLexical, fmt.Errorf("mode %q requires [search.embeddings] to be configured", requested)
		}
		return searchMode(requested), nil
	default:
		return modeLexical, fmt.Errorf("unknown mode %q (want auto|lexical|hybrid|semantic)", requested)
	}
}

// mergeRRF fuses two ranked legs with reciprocal rank fusion (k=60, equal
// weights), deduping by issue id and unioning matched_in. Ties break by RRF
// score desc, then issue id asc. The resulting Score is the RRF score.
func mergeRRF(lexical, vector []db.SearchCandidate, limit int) []db.SearchCandidate {
	type agg struct {
		issue   db.Issue
		score   float64
		matched map[string]bool
	}
	byID := map[int64]*agg{}
	add := func(leg []db.SearchCandidate) {
		for rank, c := range leg {
			a := byID[c.Issue.ID]
			if a == nil {
				a = &agg{issue: c.Issue, matched: map[string]bool{}}
				byID[c.Issue.ID] = a
			}
			a.score += 1.0 / float64(rrfK+rank+1)
			for _, m := range c.MatchedIn {
				a.matched[m] = true
			}
		}
	}
	add(lexical)
	add(vector)

	out := make([]db.SearchCandidate, 0, len(byID))
	for _, a := range byID {
		matched := make([]string, 0, len(a.matched))
		for m := range a.matched {
			matched = append(matched, m)
		}
		sort.Strings(matched)
		out = append(out, db.SearchCandidate{Issue: a.issue, Score: a.score, MatchedIn: matched})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Issue.ID < out[j].Issue.ID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/daemon/ -run 'TestMergeRRF|TestResolveMode' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/daemon/rrf.go internal/daemon/rrf_test.go
git commit -m "feat(daemon): add RRF merge and search mode resolution"
```

---

## Task 9: Reconciler

**Files:**
- Create: `internal/daemon/reconciler.go`
- Test: `internal/daemon/reconciler_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/daemon/reconciler_test.go`:

```go
package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/embedding"
)

// fakeEmbedder implements the embedder interface the reconciler depends on.
type fakeEmbedder struct {
	fp   string
	dims int
	err  error
	n    int
}

func (f *fakeEmbedder) Fingerprint() string { return f.fp }
func (f *fakeEmbedder) Dims() int           { return f.dims }
func (f *fakeEmbedder) BatchSize() int      { return 64 }
func (f *fakeEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.n += len(texts)
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = []float32{1, 0}
	}
	return out, nil
}

func TestReconcileOnceEmbedsDirtyTargets(t *testing.T) {
	ctx := context.Background()
	store := newReconcilerTestStore(t) // opens a real sqlitestore.Store
	proj, _ := store.CreateProject(ctx, "spoke-project")
	for i := 0; i < 3; i++ {
		if _, _, err := store.CreateIssue(ctx, db.CreateIssueParams{ProjectID: proj.ID, Title: "t", Body: "b", Author: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	emb := &fakeEmbedder{fp: "a" + repeat63reconciler, dims: 2}
	r := NewReconciler(store, emb, ReconcilerConfig{BatchSize: 64})

	if err := r.reconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if emb.n != 3 {
		t.Fatalf("embedded %d, want 3", emb.n)
	}
	// Second pass: nothing dirty.
	emb.n = 0
	if err := r.reconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if emb.n != 0 {
		t.Fatalf("re-embedded %d on clean pass", emb.n)
	}
	if h := r.Health(); h.Backlog != 0 || h.LastError != "" {
		t.Fatalf("unexpected health: %#v", h)
	}
}

func TestReconcileDefinitiveErrorPinsHealth(t *testing.T) {
	ctx := context.Background()
	store := newReconcilerTestStore(t)
	proj, _ := store.CreateProject(ctx, "spoke-project")
	_, _, _ = store.CreateIssue(ctx, db.CreateIssueParams{ProjectID: proj.ID, Title: "t", Body: "b", Author: "x"})
	emb := &fakeEmbedder{fp: "a" + repeat63reconciler, dims: 2, err: &embedding.APIError{StatusCode: 401, Body: "bad key"}}
	r := NewReconciler(store, emb, ReconcilerConfig{BatchSize: 64})

	err := r.reconcileOnce(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *embedding.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want APIError, got %v", err)
	}
	if h := r.Health(); h.LastError == "" {
		t.Fatal("health LastError not set")
	}
}

const repeat63reconciler = "000000000000000000000000000000000000000000000000000000000000000"

var _ = time.Second
```

Implement `newReconcilerTestStore` in the test file by opening a real `sqlitestore.Store` at a temp path (mirror the existing sqlitestore test-store helper, importing the package).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/daemon/ -run TestReconcile -v`
Expected: FAIL — `NewReconciler` undefined.

- [ ] **Step 3: Implement the reconciler**

Create `internal/daemon/reconciler.go`:

```go
package daemon

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/embedding"
)

// embedder is the subset of *embedding.Client the reconciler needs (an
// interface so tests can substitute a fake).
type embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Fingerprint() string
	Dims() int
	BatchSize() int
}

// ReconcilerConfig tunes the reconciler.
type ReconcilerConfig struct {
	BatchSize   int
	SweepEvery  time.Duration // periodic safety sweep; default 5m
	MinBackoff  time.Duration // default 1s
	MaxBackoff  time.Duration // default 5m
}

// ReconcilerHealth is the operator-visible state surfaced in /health.
type ReconcilerHealth struct {
	Configured    bool       `json:"configured"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	Backlog       int64      `json:"backlog"`
}

// Reconciler keeps issue_embeddings fresh by embedding dirty issues.
type Reconciler struct {
	store db.Storage
	emb   embedder
	cfg   ReconcilerConfig
	wake  chan struct{}

	mu     sync.Mutex
	health ReconcilerHealth
}

// NewReconciler constructs a reconciler. It does no I/O.
func NewReconciler(store db.Storage, emb embedder, cfg ReconcilerConfig) *Reconciler {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = emb.BatchSize()
	}
	if cfg.SweepEvery <= 0 {
		cfg.SweepEvery = 5 * time.Minute
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Minute
	}
	return &Reconciler{
		store:  store,
		emb:    emb,
		cfg:    cfg,
		wake:   make(chan struct{}, 1),
		health: ReconcilerHealth{Configured: true},
	}
}

// Wake nudges the reconciler to run a cycle soon (non-blocking, coalesced).
func (r *Reconciler) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Health returns a snapshot of reconciler state.
func (r *Reconciler) Health() ReconcilerHealth {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.health
}

// Run drains dirty work until ctx is cancelled, waking on Wake(), a periodic
// safety sweep, and after backoff on failure.
func (r *Reconciler) Run(ctx context.Context) error {
	backoff := r.cfg.MinBackoff
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.wake:
		case <-timer.C:
		}
		err := r.reconcileOnce(ctx)
		switch {
		case err == nil:
			backoff = r.cfg.MinBackoff
			timer.Reset(r.cfg.SweepEvery)
		default:
			backoff = r.nextBackoff(backoff, err)
			timer.Reset(backoff)
		}
	}
}

func (r *Reconciler) nextBackoff(cur time.Duration, err error) time.Duration {
	var apiErr *embedding.APIError
	if errors.As(err, &apiErr) {
		if apiErr.Definitive() {
			return r.cfg.MaxBackoff
		}
		if apiErr.RetryAfter > 0 {
			return apiErr.RetryAfter
		}
	}
	next := cur * 2
	if next > r.cfg.MaxBackoff {
		next = r.cfg.MaxBackoff
	}
	return next
}

// reconcileOnce embeds one batch of dirty targets and updates health.
func (r *Reconciler) reconcileOnce(ctx context.Context) error {
	fp := r.emb.Fingerprint()
	targets, err := r.store.ListEmbedTargets(ctx, fp, r.cfg.BatchSize)
	if err != nil {
		return err
	}
	r.setBacklog(int64(len(targets)))
	if len(targets) == 0 {
		r.markSuccess()
		return nil
	}
	texts := make([]string, len(targets))
	for i, t := range targets {
		texts[i] = embedding.EmbedText(t.Title, t.Body)
	}
	vecs, err := r.emb.Embed(ctx, texts)
	if err != nil {
		r.markError(err)
		return err
	}
	for i, t := range targets {
		if err := r.store.UpsertIssueEmbedding(ctx, db.IssueEmbedding{
			IssueID:                 t.IssueID,
			EmbeddedContentRevision: t.ContentRevision,
			Fingerprint:             fp,
			Dims:                    r.emb.Dims(),
			Vector:                  vecs[i],
		}); err != nil {
			r.markError(err)
			return err
		}
	}
	r.markSuccess()
	// More may be dirty than one batch; nudge ourselves to continue.
	if len(targets) == r.cfg.BatchSize {
		r.Wake()
	}
	return nil
}

func (r *Reconciler) markSuccess() {
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health.LastSuccessAt = &now
	r.health.LastError = ""
}

func (r *Reconciler) markError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health.LastError = err.Error()
}

func (r *Reconciler) setBacklog(n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health.Backlog = n
}
```

Note: `time.Now()` is used here in production code, which is allowed in the daemon package (the workflow-script `Date.now()` restriction does not apply to Go). Tests call `reconcileOnce` directly to avoid timing dependence.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/daemon/ -run TestReconcile -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/daemon/reconciler.go internal/daemon/reconciler_test.go
git commit -m "feat(daemon): add embedding reconciler with backoff and health"
```

---

## Task 10: Daemon wiring — ServerConfig, startup, nudge, health

**Files:**
- Modify: `internal/daemon/server.go` (`ServerConfig`)
- Modify: `internal/daemon/handlers_health.go`
- Modify: `internal/daemon/openapi.go` (`APISchemaVersion`)
- Modify: `internal/api/types.go` (`HealthResponse`)
- Modify: `cmd/kata/daemon_cmd.go` (construct + start)

- [ ] **Step 1: Extend `ServerConfig`**

In `internal/daemon/server.go`, add fields:

```go
type ServerConfig struct {
	// ... existing fields ...
	Embedder         *embedding.Client          // nil = semantic search disabled
	ReconcilerHealth func() ReconcilerHealth     // nil = disabled
}
```

Add the import `"go.kenn.io/kata/internal/embedding"`.

- [ ] **Step 2: Add health fields to the API type**

In `internal/api/types.go`, extend `HealthResponse.Body`:

```go
type HealthResponse struct {
	Body struct {
		OK               bool      `json:"ok"`
		DBPath           string    `json:"db_path"`
		SchemaVersion    int       `json:"schema_version"`
		APISchemaVersion string    `json:"api_schema_version,omitempty"`
		Version          string    `json:"version"`
		Uptime           string    `json:"uptime"`
		StartedAt        time.Time `json:"started_at"`
		Embeddings       *EmbeddingsHealth `json:"embeddings,omitempty"`
	}
}

// EmbeddingsHealth mirrors daemon.ReconcilerHealth for the wire.
type EmbeddingsHealth struct {
	Configured    bool       `json:"configured"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	Backlog       int64      `json:"backlog"`
}
```

- [ ] **Step 3: Populate health in the handler**

In `internal/daemon/handlers_health.go`, after setting the existing fields:

```go
	if cfg.ReconcilerHealth != nil {
		h := cfg.ReconcilerHealth()
		out.Body.Embeddings = &api.EmbeddingsHealth{
			Configured:    h.Configured,
			LastSuccessAt: h.LastSuccessAt,
			LastError:     h.LastError,
			Backlog:       h.Backlog,
		}
	}
```

- [ ] **Step 4: Bump the API schema version**

In `internal/daemon/openapi.go:15`:

```go
const APISchemaVersion = "0.3.0"
```

- [ ] **Step 5: Write the wiring test (health reflects reconciler)**

Add to `internal/daemon/handlers_health_test.go` (or create it) a test that builds a `ServerConfig` with a `ReconcilerHealth` func returning `{Configured: true, Backlog: 5}` and asserts the health response includes `embeddings.backlog == 5`. Use the existing daemon test harness (look for how other handler tests construct the huma test API in `internal/daemon/*_test.go`).

```go
func TestHealthIncludesEmbeddings(t *testing.T) {
	cfg := newTestServerConfig(t) // existing harness
	cfg.ReconcilerHealth = func() ReconcilerHealth {
		return ReconcilerHealth{Configured: true, Backlog: 5}
	}
	resp := doHealth(t, cfg) // existing harness call; returns api.HealthResponse body
	if resp.Embeddings == nil || resp.Embeddings.Backlog != 5 {
		t.Fatalf("embeddings health missing: %#v", resp.Embeddings)
	}
}
```

Adapt to the real harness names found in the test files.

- [ ] **Step 6: Run the health test**

Run: `go test ./internal/daemon/ -run TestHealthIncludesEmbeddings -v`
Expected: PASS.

- [ ] **Step 7: Construct client + reconciler at daemon start**

In `cmd/kata/daemon_cmd.go`, inside `runDaemonWithListen` after the broadcaster is created and before `NewServer`, add:

```go
	var embedder *embedding.Client
	var reconcilerHealth func() daemon.ReconcilerHealth
	if dcfg.Search.Embeddings.Enabled() {
		ec := dcfg.Search.Embeddings
		embedder, err = embedding.New(embedding.Config{
			BaseURL:             ec.BaseURL,
			Model:               ec.Model,
			APIKey:              ec.ResolvedAPIKey(),
			Salt:                ec.FingerprintSalt,
			Dims:                ec.Dims,
			BatchSize:           ec.BatchSize,
			Timeout:             time.Duration(ec.TimeoutSeconds) * time.Second,
			TrustPrivateNetwork: ec.TrustPrivateNetwork,
		})
		if err != nil {
			return fmt.Errorf("embedding client: %w", err)
		}
		reconciler := daemon.NewReconciler(store, embedder, daemon.ReconcilerConfig{BatchSize: ec.BatchSize})
		reconcilerHealth = reconciler.Health
		// Start the reconciler and nudge it on every event.
		go func() {
			if err := reconciler.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				daemonLog.Printf("reconciler: %v", err)
			}
		}()
		startEmbeddingNudge(ctx, broadcaster, reconciler)
		reconciler.Wake() // initial backfill sweep
	}
```

Then pass into `NewServer(daemon.ServerConfig{... Embedder: embedder, ReconcilerHealth: reconcilerHealth})`.

Add `startEmbeddingNudge` near `startFederationRunner` in the same file, mirroring the broadcaster-subscription pattern at `cmd/kata/daemon_cmd.go:481`:

```go
// startEmbeddingNudge subscribes to the broadcaster and wakes the reconciler
// on every committed event so new/edited issues are embedded promptly.
func startEmbeddingNudge(ctx context.Context, bcast *daemon.EventBroadcaster, r *daemon.Reconciler) {
	sub := bcast.Subscribe(daemon.SubFilter{})
	go func() {
		defer sub.Unsub()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-sub.Ch:
				if !ok {
					return
				}
				if msg.Kind == "event" {
					r.Wake()
				}
			}
		}
	}()
}
```

Confirm the exact `Subscribe`/`SubFilter`/`Unsub`/`Ch` names from `internal/daemon/broadcaster.go` and adjust (the federation runner at `daemon_cmd.go:481` is the reference usage). Add imports: `"go.kenn.io/kata/internal/embedding"`, `"errors"`, `"time"` if not present.

- [ ] **Step 8: Build the daemon**

Run: `go build ./cmd/kata/ && go vet ./internal/daemon/ ./cmd/kata/`
Expected: no errors.

- [ ] **Step 9: Commit**

```bash
git add internal/daemon/server.go internal/daemon/handlers_health.go internal/daemon/openapi.go internal/api/types.go internal/daemon/handlers_health_test.go cmd/kata/daemon_cmd.go
git commit -m "feat(daemon): wire embedding client, reconciler, and health reporting"
```

---

## Task 11: Search handler — hybrid orchestration + API types

**Files:**
- Create: `internal/daemon/hybrid_search.go`
- Modify: `internal/daemon/handlers_search.go`
- Modify: `internal/api/types.go` (`SearchRequest`, `SearchResponse`)
- Test: `internal/daemon/hybrid_search_test.go`

- [ ] **Step 1: Extend the API types**

In `internal/api/types.go`:

```go
type SearchRequest struct {
	ProjectID      int64  `path:"project_id" required:"true"`
	Query          string `query:"q" required:"true"`
	Limit          int    `query:"limit,omitempty"`
	IncludeDeleted bool   `query:"include_deleted,omitempty"`
	Mode           string `query:"mode,omitempty" enum:"auto,lexical,hybrid,semantic"`
}

// SearchHit: Score is mode-scoped — negated BM25 (lexical), RRF score
// (hybrid), or cosine similarity (semantic). MatchedIn includes "semantic"
// when the vector leg contributed.
type SearchHit struct {
	Issue     db.Issue `json:"issue"`
	Score     float64  `json:"score"`
	MatchedIn []string `json:"matched_in"`
}

type SearchResponse struct {
	Body struct {
		Query          string      `json:"query"`
		Mode           string      `json:"mode"`
		Degraded       bool        `json:"degraded,omitempty"`
		DegradedReason string      `json:"degraded_reason,omitempty"`
		Results        []SearchHit `json:"results"`
	}
}
```

- [ ] **Step 2: Write the failing orchestration test**

Create `internal/daemon/hybrid_search_test.go`:

```go
package daemon

import (
	"context"
	"testing"

	"go.kenn.io/kata/internal/db"
)

func TestHybridSearchLexicalWhenUnconfigured(t *testing.T) {
	ctx := context.Background()
	store := newReconcilerTestStore(t)
	proj, _ := store.CreateProject(ctx, "spoke-project")
	_, _, _ = store.CreateIssue(ctx, db.CreateIssueParams{ProjectID: proj.ID, Title: "login race", Body: "x", Author: "a"})

	res, err := hybridSearch(ctx, store, nil /*embedder*/, hybridParams{
		ProjectID: proj.ID, Query: "login", Limit: 10, Requested: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != modeLexical || res.Degraded {
		t.Fatalf("unconfigured should be lexical, not degraded: %#v", res)
	}
}

func TestHybridSearchExplicitHybridUnconfiguredErrors(t *testing.T) {
	ctx := context.Background()
	store := newReconcilerTestStore(t)
	proj, _ := store.CreateProject(ctx, "spoke-project")
	_, err := hybridSearch(ctx, store, nil, hybridParams{ProjectID: proj.ID, Query: "x", Limit: 10, Requested: "hybrid"})
	if err == nil {
		t.Fatal("expected 400-class error for explicit hybrid without embeddings")
	}
}
```

(A degraded-path test with a failing embedder is added once `hybridSearch` exists; include it here too if the embedder interface allows a fake. The handler-level degraded test lives in Task 12's CLI tests and an e2e.)

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/daemon/ -run TestHybridSearch -v`
Expected: FAIL — `hybridSearch` undefined.

- [ ] **Step 4: Implement the orchestration**

Create `internal/daemon/hybrid_search.go`:

```go
package daemon

import (
	"context"
	"time"

	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/embedding"
)

type hybridParams struct {
	ProjectID      int64
	Query          string
	Limit          int
	IncludeDeleted bool
	Requested      string // raw mode param
}

type hybridResult struct {
	Mode           searchMode
	Degraded       bool
	DegradedReason string
	Hits           []db.SearchCandidate
}

const queryEmbedTimeout = 3 * time.Second

// hybridSearch runs the lexical and (when applicable) vector legs and merges
// them. The lexical leg never waits on the embedder. A vector-leg failure in
// an auto/hybrid request degrades to lexical with a reason; an explicit
// hybrid/semantic request that cannot run returns an error for the handler to
// map to 400 (unconfigured) or 503 (leg failure).
func hybridSearch(ctx context.Context, store db.Storage, emb *embedding.Client, p hybridParams) (hybridResult, error) {
	configured := emb != nil
	mode, err := resolveMode(p.Requested, configured)
	if err != nil {
		return hybridResult{}, &modeError{status: 400, msg: err.Error()}
	}
	// strict = the caller explicitly asked for a leg that must run; a failure
	// is 503, not a silent degrade. auto-resolved hybrid is not strict.
	strict := p.Requested == "hybrid" || p.Requested == "semantic"

	fetch := p.Limit * 3
	if fetch < 50 {
		fetch = 50
	}
	if fetch > 200 {
		fetch = 200
	}

	// Lexical leg (skip for explicit semantic).
	var lexical []db.SearchCandidate
	lexErrCh := make(chan error, 1)
	if mode != modeSemantic {
		go func() {
			c, e := store.SearchFTS(ctx, p.ProjectID, p.Query, fetch, p.IncludeDeleted)
			lexical = c
			lexErrCh <- e
		}()
	} else {
		lexErrCh <- nil
	}

	// Vector leg.
	var vector []db.SearchCandidate
	var vecErr error
	if mode == modeHybrid || mode == modeSemantic {
		vector, vecErr = runVectorLeg(ctx, store, emb, p, fetch)
	}

	if e := <-lexErrCh; e != nil {
		return hybridResult{}, &modeError{status: 500, msg: e.Error()}
	}

	// Handle vector-leg failure. Explicit hybrid/semantic → 503; auto → degrade
	// to lexical, labeled (keeps "silent-but-labeled" honest).
	if vecErr != nil {
		if strict {
			return hybridResult{}, &modeError{status: 503, msg: vecErr.Error()}
		}
		return hybridResult{
			Mode: modeLexical, Degraded: true, DegradedReason: vecErr.Error(),
			Hits: truncate(lexical, p.Limit),
		}, nil
	}

	switch mode {
	case modeLexical:
		return hybridResult{Mode: modeLexical, Hits: truncate(lexical, p.Limit)}, nil
	case modeSemantic:
		return hybridResult{Mode: modeSemantic, Hits: truncate(vector, p.Limit)}, nil
	default: // hybrid
		return hybridResult{Mode: modeHybrid, Hits: mergeRRF(lexical, vector, p.Limit)}, nil
	}
}

func runVectorLeg(ctx context.Context, store db.Storage, emb *embedding.Client, p hybridParams, fetch int) ([]db.SearchCandidate, error) {
	ectx, cancel := context.WithTimeout(ctx, queryEmbedTimeout)
	defer cancel()
	vecs, err := emb.Embed(ectx, []string{embedding.EmbedText(p.Query, "")})
	if err != nil {
		return nil, err
	}
	hits, err := store.SearchVector(ctx, p.ProjectID, vecs[0], emb.Fingerprint(), fetch, p.IncludeDeleted)
	if err != nil {
		return nil, err
	}
	// Drop weak vectors so they do not pad results.
	const floor = 0.3
	out := hits[:0]
	for _, h := range hits {
		if h.Score >= floor {
			out = append(out, h)
		}
	}
	return out, nil
}

func truncate(c []db.SearchCandidate, limit int) []db.SearchCandidate {
	if limit > 0 && len(c) > limit {
		return c[:limit]
	}
	return c
}

// modeError carries an HTTP status for the handler to translate.
type modeError struct {
	status int
	msg    string
}

func (e *modeError) Error() string { return e.msg }
func (e *modeError) Status() int   { return e.status }
```

The `strict` bool (computed once from `p.Requested`) is what distinguishes an explicit `hybrid`/`semantic` request — which must return 503 on a vector-leg failure — from an `auto`-resolved hybrid, which degrades to labeled lexical. The Step 2 test plus a degrade test pin both behaviors.

- [ ] **Step 5: Rewrite the search handler to use it**

Replace the handler body in `internal/daemon/handlers_search.go`:

```go
func registerSearchHandlers(humaAPI huma.API, cfg ServerConfig) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "searchIssues",
		Method:      "GET",
		Path:        "/api/v1/projects/{project_id}/search",
	}, func(ctx context.Context, in *api.SearchRequest) (*api.SearchResponse, error) {
		if strings.TrimSpace(in.Query) == "" {
			return nil, api.NewError(400, "validation", "query parameter q must be non-empty", "", nil)
		}
		if _, err := activeProjectByID(ctx, cfg.DB, in.ProjectID); err != nil {
			return nil, err
		}
		limit := in.Limit
		if limit <= 0 {
			limit = 20
		}
		res, err := hybridSearch(ctx, cfg.DB, cfg.Embedder, hybridParams{
			ProjectID: in.ProjectID, Query: in.Query, Limit: limit,
			IncludeDeleted: in.IncludeDeleted, Requested: in.Mode,
		})
		if err != nil {
			var me *modeError
			if errors.As(err, &me) {
				kind := "validation"
				if me.Status() == 503 {
					kind = "unavailable"
				}
				return nil, api.NewError(me.Status(), kind, me.Error(), "", nil)
			}
			return nil, api.NewError(500, "internal", err.Error(), "", nil)
		}
		out := &api.SearchResponse{}
		out.Body.Query = in.Query
		out.Body.Mode = string(res.Mode)
		out.Body.Degraded = res.Degraded
		out.Body.DegradedReason = res.DegradedReason
		out.Body.Results = make([]api.SearchHit, 0, len(res.Hits))
		for _, c := range res.Hits {
			out.Body.Results = append(out.Body.Results, api.SearchHit{Issue: c.Issue, Score: c.Score, MatchedIn: c.MatchedIn})
		}
		return out, nil
	})
}
```

Add `"errors"` to imports. Confirm the registration function name matches what `registerRoutes` calls (it may be `registerSearch`, not `registerSearchHandlers` — match the existing call site in `server.go`).

- [ ] **Step 6: Run tests**

Run: `go test ./internal/daemon/ -run 'TestHybridSearch|TestMergeRRF|TestResolveMode' -v`
Expected: PASS.

- [ ] **Step 7: Build**

Run: `go build ./... && go vet ./internal/daemon/`
Expected: no errors.

- [ ] **Step 8: Commit**

```bash
git add internal/daemon/hybrid_search.go internal/daemon/handlers_search.go internal/api/types.go internal/daemon/hybrid_search_test.go
git commit -m "feat(daemon): hybrid search orchestration with mode and degraded contract"
```

---

## Task 12: CLI — flags and output rendering

**Files:**
- Modify: `cmd/kata/search.go`
- Test: `cmd/kata/search_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `cmd/kata/search_test.go`:

```go
func TestSearchHumanBaselineLexicalUnchanged(t *testing.T) {
	// Simulate a lexical response (no embeddings).
	body := `{"query":"login","mode":"lexical","results":[
	  {"issue":{"short_id":"abc4","title":"Fix login","status":"open"},"score":1.23,"matched_in":["title"]}]}`
	out := renderSearch(t, outputHuman, body)
	// No header line; existing %.2f row format preserved.
	if strings.Contains(out, "# mode=") {
		t.Fatalf("baseline lexical must not print a mode header:\n%s", out)
	}
	if !strings.Contains(out, "abc4") || !strings.Contains(out, "1.23") {
		t.Fatalf("row format changed:\n%s", out)
	}
}

func TestSearchHumanDegradedAutoPrintsNote(t *testing.T) {
	body := `{"query":"login","mode":"lexical","degraded":true,"degraded_reason":"embedder unreachable","results":[]}`
	out := renderSearch(t, outputHuman, body)
	if !strings.Contains(out, "# mode=lexical") || !strings.Contains(out, "degraded") {
		t.Fatalf("degraded auto must print a note:\n%s", out)
	}
}

func TestSearchHumanHybridUsesHigherPrecision(t *testing.T) {
	body := `{"query":"login","mode":"hybrid","results":[
	  {"issue":{"short_id":"abc4","title":"Fix login","status":"open"},"score":0.0163,"matched_in":["title","semantic"]}]}`
	out := renderSearch(t, outputHuman, body)
	if !strings.Contains(out, "# mode=hybrid") {
		t.Fatalf("hybrid must print mode header:\n%s", out)
	}
	if !strings.Contains(out, "0.0163") {
		t.Fatalf("hybrid must use %%.4f precision:\n%s", out)
	}
}

func TestSearchAgentAppendsMode(t *testing.T) {
	body := `{"query":"login","mode":"lexical","results":[
	  {"issue":{"short_id":"abc4","title":"Fix login","status":"open"},"score":1.2,"matched_in":["title"]}]}`
	out := renderSearch(t, outputAgent, body)
	// Appended after count= and query=, in order.
	if !strings.Contains(out, "OK search count=1 query=login mode=lexical") {
		t.Fatalf("agent header order wrong:\n%s", out)
	}
}
```

Implement a small `renderSearch(t, mode, body)` test helper that sets the output mode and calls `printSearchResults` with the body bytes, capturing stdout. Mirror how other `cmd/kata` tests capture command output (`testhelpers_test.go`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/kata/ -run TestSearch -v`
Expected: FAIL — new fields/rendering not present.

- [ ] **Step 3: Add the flags**

In `cmd/kata/search.go` `newSearchCmd`, add:

```go
	var lexical, hybrid, semantic bool
	// ... inside RunE, after the empty-query check:
		modeFlags := 0
		for _, b := range []bool{lexical, hybrid, semantic} {
			if b {
				modeFlags++
			}
		}
		if modeFlags > 1 {
			return &cliError{Message: "--lexical, --hybrid, and --semantic are mutually exclusive", Kind: kindValidation, ExitCode: ExitValidation}
		}
		mode := ""
		switch {
		case lexical:
			mode = "lexical"
		case hybrid:
			mode = "hybrid"
		case semantic:
			mode = "semantic"
		}
	// ... and register the flags before returning cmd:
	cmd.Flags().BoolVar(&lexical, "lexical", false, "lexical (FTS) search only")
	cmd.Flags().BoolVar(&hybrid, "hybrid", false, "hybrid lexical+semantic search")
	cmd.Flags().BoolVar(&semantic, "semantic", false, "semantic (vector) search only")
```

Thread `mode` into `buildSearchURL` by adding a `mode` param:

```go
func buildSearchURL(baseURL string, pid int64, query string, limit int, includeDeleted bool, mode string) string {
	q := url.Values{}
	q.Set("q", query)
	if limit > 0 {
		q.Set("limit", fmt.Sprint(limit))
	}
	if includeDeleted {
		q.Set("include_deleted", "true")
	}
	if mode != "" {
		q.Set("mode", mode)
	}
	return fmt.Sprintf("%s/api/v1/projects/%d/search?%s", baseURL, pid, q.Encode())
}
```

Update the call site to pass `mode`.

- [ ] **Step 4: Update `printSearchResults`**

Extend the decoded struct and the human/agent branches in `cmd/kata/search.go`:

```go
	var b struct {
		Query          string `json:"query"`
		Mode           string `json:"mode"`
		Degraded       bool   `json:"degraded"`
		DegradedReason string `json:"degraded_reason"`
		Results        []struct {
			Issue struct {
				ShortID string `json:"short_id"`
				Title   string `json:"title"`
				Status  string `json:"status"`
			} `json:"issue"`
			Score     float64  `json:"score"`
			MatchedIn []string `json:"matched_in"`
		} `json:"results"`
	}
```

Agent branch — append `mode=` (and `degraded=` when set) to the `OK search` line, preserving `count=`/`query=` order:

```go
	if mode == outputAgent {
		out := cmd.OutOrStdout()
		header := fmt.Sprintf("OK search count=%d query=%s mode=%s", len(b.Results), agentValue(b.Query), b.Mode)
		if b.Degraded {
			header += " degraded=" + agentValue(b.DegradedReason)
		}
		if _, err := fmt.Fprintln(out, header); err != nil {
			return err
		}
		for _, r := range b.Results {
			if err := writeAgentKVRow(out,
				agentRowField("issue", r.Issue.ShortID),
				agentRowFloatField("score", r.Score),
				agentRowField("status", r.Issue.Status),
				agentRowListField("matched", r.MatchedIn),
				agentRowField("title", r.Issue.Title),
			); err != nil {
				return err
			}
		}
		return nil
	}
```

Human branch — print a header only when not baseline lexical; use `%.4f` for hybrid/semantic:

```go
	// Header rule: print only when output is NOT plain baseline lexical, i.e.
	// mode != lexical OR degraded. Baseline lexical stays byte-identical.
	if b.Mode != "lexical" || b.Degraded {
		header := "# mode=" + b.Mode
		if b.Degraded {
			header += " degraded: " + b.DegradedReason
		}
		if _, err := fmt.Fprintln(cmd.OutOrStdout(), header); err != nil {
			return err
		}
	}
	if len(b.Results) == 0 {
		if flags.Quiet {
			return nil
		}
		_, err := fmt.Fprintln(cmd.OutOrStdout(), "no matches")
		return err
	}
	scoreFmt := "%.2f"
	if b.Mode == "hybrid" || b.Mode == "semantic" {
		scoreFmt = "%.4f"
	}
	for _, r := range b.Results {
		line := fmt.Sprintf("%-8s  "+scoreFmt+"  %-8s  %s  (%s)\n",
			r.Issue.ShortID, r.Score, r.Issue.Status,
			textsafe.Line(r.Issue.Title), strings.Join(r.MatchedIn, ","))
		if _, err := fmt.Fprint(cmd.OutOrStdout(), line); err != nil {
			return err
		}
	}
	return nil
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./cmd/kata/ -run TestSearch -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/kata/search.go cmd/kata/search_test.go
git commit -m "feat(cli): add search mode flags and degraded-aware rendering"
```

---

## Task 13: JSONL export/import for embeddings

**Files:**
- Modify: `internal/db/export_types.go` (`IssueExport.ContentRevision`, `IssueEmbeddingExport`)
- Modify: `internal/db/sqlitestore/export.go` (`ExportIssues` selects `content_revision`)
- Create: `internal/db/sqlitestore/export_embeddings.go` (`ExportIssueEmbeddings`)
- Modify: `internal/db/storage.go` (add `ExportIssueEmbeddings`)
- Modify: `internal/db/pgstore/stubs_gen.go` (stub)
- Modify: `internal/jsonl/types.go` (kind), `internal/jsonl/storage_export.go` (emit), `internal/jsonl/import.go` (map + content_revision)
- Test: `internal/jsonl/roundtrip_embeddings_test.go`

- [ ] **Step 1: Write the failing round-trip test**

Create `internal/jsonl/roundtrip_embeddings_test.go`:

```go
package jsonl

import (
	"bytes"
	"context"
	"testing"

	"go.kenn.io/kata/internal/db"
)

func TestEmbeddingsRoundTripPreservesContentRevision(t *testing.T) {
	ctx := context.Background()
	src := newJSONLTestStore(t) // opens a real sqlitestore.Store
	proj, _ := src.CreateProject(ctx, "spoke-project")
	iss, _, _ := src.CreateIssue(ctx, db.CreateIssueParams{ProjectID: proj.ID, Title: "t", Body: "b", Author: "a"})
	nt := "edited"
	_, _, _, _ = src.EditIssue(ctx, db.EditIssueParams{IssueID: iss.ID, Title: &nt, Actor: "a"})
	var cr int64
	_ = src.QueryRowContext(ctx, `SELECT content_revision FROM issues WHERE id=?`, iss.ID).Scan(&cr)
	_ = src.UpsertIssueEmbedding(ctx, db.IssueEmbedding{
		IssueID: iss.ID, EmbeddedContentRevision: cr, Fingerprint: "a" + repeat63jsonl, Dims: 2, Vector: []float32{1, 0},
	})

	var buf bytes.Buffer
	if err := Export(ctx, src, &buf, ExportOptions{}); err != nil {
		t.Fatal(err)
	}

	dst := newJSONLTestStore(t)
	if err := ImportWithOptions(ctx, &buf, dst, ImportOptions{}); err != nil {
		t.Fatal(err)
	}

	// Imported issue carries content_revision; embedding is NOT falsely stale.
	targets, _ := dst.ListEmbedTargets(ctx, "a"+repeat63jsonl, 10)
	if len(targets) != 0 {
		t.Fatalf("imported embedding falsely stale: %#v", targets)
	}
}

const repeat63jsonl = "000000000000000000000000000000000000000000000000000000000000000"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/jsonl/ -run TestEmbeddingsRoundTrip -v`
Expected: FAIL — `content_revision`/embedding export not present (imported issue defaults to content_revision 0, so the embedding is stale).

- [ ] **Step 3: Add `ContentRevision` to `IssueExport` and the embedding export type**

In `internal/db/export_types.go`, add to `IssueExport` (after `Revision`):

```go
	Revision        int64  `json:"revision"`
	ContentRevision int64  `json:"content_revision"`
```

And add:

```go
// IssueEmbeddingExport is one embedding row in export shape, keyed by issue
// UID so import resolves identity independent of local numeric ids.
type IssueEmbeddingExport struct {
	IssueUID                string `json:"issue_uid"`
	EmbeddedContentRevision int64  `json:"embedded_content_revision"`
	Fingerprint             string `json:"embed_fingerprint"`
	Dims                    int    `json:"dims"`
	VectorB64               string `json:"vector_b64"`
}
```

- [ ] **Step 4: Select `content_revision` in `ExportIssues`**

In `internal/db/sqlitestore/export.go`, add `i.content_revision` to the `ExportIssues` query and scan it into `rec.ContentRevision` (place it next to `i.revision`/`&rec.Revision`).

- [ ] **Step 5: Implement `ExportIssueEmbeddings`**

Create `internal/db/sqlitestore/export_embeddings.go`:

```go
package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/base64"
	"iter"

	"go.kenn.io/kata/internal/db"
)

// ExportIssueEmbeddings streams embedding rows joined to their issue UID.
// Live-only (non-include-deleted) exports omit rows whose parent issue is
// excluded, via the same WHERE the issue export uses.
func (d *Store) ExportIssueEmbeddings(ctx context.Context, f db.ExportFilter) iter.Seq2[db.IssueEmbeddingExport, error] {
	query := `SELECT i.uid, e.embedded_content_revision, e.embed_fingerprint, e.dims, e.vector_bytes
	          FROM issue_embeddings e
	          JOIN issues i ON i.id = e.issue_id` +
		exportWhere("i", f) + ` ORDER BY i.id ASC`
	return streamRows(ctx, d.readQ, "issue_embeddings", query, exportArgs(f),
		func(rows *sql.Rows) (db.IssueEmbeddingExport, error) {
			var rec db.IssueEmbeddingExport
			var raw []byte
			if err := rows.Scan(&rec.IssueUID, &rec.EmbeddedContentRevision, &rec.Fingerprint, &rec.Dims, &raw); err != nil {
				return db.IssueEmbeddingExport{}, scanError("issue_embedding", err)
			}
			rec.VectorB64 = base64.StdEncoding.EncodeToString(raw)
			return rec, nil
		})
}
```

Add `ExportIssueEmbeddings` to the `Storage` interface in `internal/db/storage.go` (export group) and stub it in `pgstore/stubs_gen.go`:

```go
func (s *Store) ExportIssueEmbeddings(_ context.Context, _ db.ExportFilter) iter.Seq2[db.IssueEmbeddingExport, error] {
	return func(yield func(db.IssueEmbeddingExport, error) bool) {
		yield(db.IssueEmbeddingExport{}, errNotImplemented("ExportIssueEmbeddings"))
	}
}
```

- [ ] **Step 6: Add the JSONL kind and emit it**

In `internal/jsonl/types.go`, add `KindIssueEmbedding Kind = "issue_embedding"` and insert it into the `kindOrder` map at the correct rank — **after `KindIssue`** (embeddings depend on issues existing). Renumber subsequent ranks accordingly, or append at the end if the import replay is order-tolerant for this kind (verify by reading how `kindOrder` ranks are consumed; safest is to slot right after issue and shift the rest).

In `internal/jsonl/storage_export.go` `Export`, add a `streamExport` call for the new kind, placed right after the issues export:

```go
	if err := streamExport(enc, KindIssue, store.ExportIssues(ctx, f)); err != nil {
		return err
	}
	if err := streamExport(enc, KindIssueEmbedding, store.ExportIssueEmbeddings(ctx, f)); err != nil {
		return err
	}
```

- [ ] **Step 7: Map the kind on import and carry content_revision**

In `internal/jsonl/import.go` `toImportRecord`, add a case for `KindIssueEmbedding` that decodes `IssueEmbeddingExport`, base64-decodes the vector, and produces a `db.ImportRecord` carrying it. Then in `db.ImportReplay` (sqlitestore implementation), handle the new record: resolve `issue_uid` → issue id, validate `embedded_content_revision <= issues.content_revision`, and insert into `issue_embeddings` (skip rows failing validation; they will be re-embedded). Ensure the issue replay writes `content_revision` from `IssueExport.ContentRevision` (it currently writes other issue columns — add `content_revision`).

Because `ImportReplay` is backend-specific, locate the sqlitestore replay (search `rg -n "func.*ImportReplay" internal/db/sqlitestore`) and add the embedding insert + the issue `content_revision` column there.

- [ ] **Step 8: Run the round-trip test**

Run: `go test ./internal/jsonl/ -run TestEmbeddingsRoundTrip -v`
Expected: PASS.

- [ ] **Step 9: Run the full JSONL + db suites**

Run: `go test ./internal/jsonl/ ./internal/db/... 2>&1 | tail -25`
Expected: PASS. (Existing export/import determinism tests must still pass; if an export golden file lists record kinds, update it to include `issue_embedding`.)

- [ ] **Step 10: Commit**

```bash
git add internal/db/export_types.go internal/db/sqlitestore/export.go internal/db/sqlitestore/export_embeddings.go internal/db/storage.go internal/db/pgstore/stubs_gen.go internal/jsonl/types.go internal/jsonl/storage_export.go internal/jsonl/import.go internal/jsonl/roundtrip_embeddings_test.go
git commit -m "feat(jsonl): export and import issue embeddings with content_revision"
```

---

## Task 14: End-to-end and final verification

**Files:**
- Test: `e2e/semantic_search_test.go` (or the repo's e2e location — check `e2e/`)

- [ ] **Step 1: Write an e2e with a deterministic fake embedder**

Create an e2e test that:
1. Starts a daemon configured with `[search.embeddings]` pointing at an in-test `httptest` server that returns deterministic vectors keyed by input text (e.g. hash text → fixed vector so a paraphrase maps near its source).
2. Creates an issue; asserts `kata search` finds it **lexically immediately** (before reconcile).
3. Waits for the reconciler (poll `/health` until `embeddings.backlog == 0`).
4. Searches a paraphrase; asserts the issue is found via `mode=hybrid` with `semantic` in `matched`.
5. Kills the embedder (close the httptest server); asserts `kata search` returns results with `mode=lexical degraded`.

Use neutral placeholder names (`spoke-project`, etc.) per repo rules. Model the harness on existing tests in `e2e/`.

- [ ] **Step 2: Run the e2e**

Run: `go test ./e2e/ -run SemanticSearch -v`
Expected: PASS.

- [ ] **Step 3: Full suite, vet, lint**

Run: `go build ./... && go test ./... 2>&1 | tail -30 && go vet ./...`
Expected: all PASS, no vet warnings. Run `golangci-lint run` if configured and fix findings.

- [ ] **Step 4: Manual smoke against a real endpoint (optional, documented)**

If an Ollama instance is available:

```bash
# config.toml: [search.embeddings] base_url="http://localhost:11434/v1" model="nomic-embed-text"
kata daemon start &
kata create "Login callback double-submits on Safari" --body "race condition"
sleep 5   # let the reconciler embed
kata search "auth redirect duplicates" --hybrid   # should surface the issue via semantic leg
```

This is a documented manual check, not an automated test (no network in CI).

- [ ] **Step 5: Update docs**

In `docs/reference/cli.md`, document the new `--lexical`/`--hybrid`/`--semantic` flags for `kata search`. In `docs/reference/agent-output.md`, note that `OK search` now appends `mode=` (and `degraded=` when applicable) and that rows may include `semantic` in `matched=`. Add a short `[search.embeddings]` section to the operations/config docs describing opt-in, the privacy note (issue text is sent to the endpoint; prefer localhost for sensitive projects), and that embeddings do not federate.

- [ ] **Step 6: Commit**

```bash
git add e2e/ docs/reference/cli.md docs/reference/agent-output.md docs/operations/
git commit -m "test(e2e): semantic search end-to-end; document config and flags"
```

---

## Done criteria

- `kata search` with no `[search.embeddings]` config behaves byte-for-byte as before (pinned by compatibility tests); `api_schema_version` is `0.3.0` and `/health` reports `embeddings` only when configured.
- With embeddings configured: new issues are searchable lexically immediately and semantically within seconds; paraphrase queries surface conceptually-related issues; an unreachable embedder degrades `auto` to labeled lexical and rejects explicit `hybrid`/`semantic` with 503.
- `content_revision` bumps on title/body edits from all writers and nothing else; model swaps re-embed gradually under a new fingerprint without cross-model comparison; JSONL round-trips embeddings without false staleness.
- All new code is TDD'd; `go build ./...`, `go test ./...`, and `go vet ./...` are clean.

**Next:** Phase 2 plan — PostgreSQL `SearchFTS`/`SearchFTSAny` over the existing tsvector machinery, pgvector best-effort acceleration (advisory-locked DDL, brute-force fallback), and extending the storage conformance suite so pgstore runs the same embedding tests.
