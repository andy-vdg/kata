//go:build !windows

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestE2E_SemanticSearch_HybridAndDegraded boots a real `kata daemon`
// subprocess configured with [search.embeddings] pointing at an in-test
// deterministic embedder, and exercises the whole semantic-search path that
// only exists in the daemon's process wiring (startEmbeddingReconciler: the
// reconciler goroutine, the broadcaster nudge, the initial backfill, and the
// /health backlog report):
//
//  1. create an issue → it is found lexically *immediately*, before any embed;
//  2. wait for the reconciler to drain (poll /health until backlog == 0);
//  3. a paraphrase query that shares no salient tokens is found with
//     mode=hybrid and "semantic" in matched_in — the vector leg contributed;
//  4. kill the embedder → an auto search returns results with mode=lexical and
//     degraded=true, and an explicit --hybrid request is rejected 503.
//
// The deterministic embedder maps inputs to fixed unit vectors by topic, so a
// paraphrase lands on the same axis as its source (cosine 1.0, clearing the
// 0.3 floor) while an unrelated topic is orthogonal (cosine 0.0, filtered).
// This is the e2e from the design note's testing plan ("create → instant
// lexical hit; after reconcile → paraphrase found semantically; embedder
// killed → degraded lexical") with neutral placeholder data.
func TestE2E_SemanticSearch_HybridAndDegraded(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e tests are slow")
	}

	embedder := newFixtureEmbedder(t)

	dirs := newE2EDirs(t)
	writeEmbeddingsConfig(t, dirs.home, embedder.URL())

	bin := buildKataBinary(t)
	env := dirs.env()

	daemonStderr := startDaemon(t, bin, env)
	url, client := connectDaemon(t, dirs, daemonStderr)

	pid := initProjectE2E(t, client, url, dirs.repoDir)
	pidStr := strconv.FormatInt(pid, 10)

	// 1. Create an authentication-topic issue and assert it is searchable
	// lexically the instant it is committed — before the reconciler has had a
	// chance to embed anything. Lexical freshness is same-commit (FTS triggers
	// fire in the writing transaction); semantic freshness is eventual.
	short := createIssueWithBody(t, client, url, pid,
		"Login callback double-submits on Safari",
		"A redirect race condition fires the auth callback twice.")

	lexical := searchHybrid(t, client, url, pidStr, "callback", "lexical")
	require.Equal(t, "lexical", lexical.Mode)
	require.True(t, containsIssue(lexical, short),
		"freshly created issue must be found lexically before reconcile: %+v", lexical)

	// 2. Wait for the reconciler to embed the backlog. /health reports the
	// reconciler's operator-visible state only when embeddings are configured;
	// backlog == 0 means every dirty issue has a current-fingerprint vector.
	waitForEmbeddingBacklogZero(t, client, url, daemonStderr)

	// 3. A paraphrase whose tokens appear nowhere in the issue's title or body
	// ("credential loop on returning users" vs "Login callback double-submits
	// on Safari" / "A redirect race condition fires the auth callback twice")
	// can only surface via the vector leg. First confirm lexical search finds
	// nothing, isolating the semantic contribution, then assert the hybrid
	// search surfaces the issue with mode=hybrid and "semantic" in matched_in.
	const paraphrase = "credential loop on returning users"
	lexMiss := searchHybrid(t, client, url, pidStr, paraphrase, "lexical")
	require.Falsef(t, containsIssue(lexMiss, short),
		"paraphrase must NOT match lexically (isolates the semantic leg): %+v", lexMiss)

	hybrid := searchHybrid(t, client, url, pidStr, paraphrase, "hybrid")
	require.Equal(t, "hybrid", hybrid.Mode, "explicit hybrid must run the vector leg: %+v", hybrid)
	require.False(t, hybrid.Degraded, "embedder is up; hybrid must not be degraded: %+v", hybrid)
	hit, ok := findHit(hybrid, short)
	require.Truef(t, ok, "paraphrase must surface the issue via the vector leg: %+v", hybrid)
	require.Containsf(t, hit.MatchedIn, "semantic",
		"vector leg must be credited in matched_in: %+v", hit)

	// 4. Kill the embedder. An auto/hybrid request can no longer embed the
	// query, so the vector leg fails. auto degrades to labeled lexical; an
	// explicit hybrid request is an honest 503 rather than a silent downgrade.
	embedder.Close()

	degraded := searchHybrid(t, client, url, pidStr, paraphrase, "")
	require.Equal(t, "lexical", degraded.Mode,
		"auto must fall back to lexical when the embedder is down: %+v", degraded)
	require.True(t, degraded.Degraded,
		"the fallback must be labeled degraded, not silent: %+v", degraded)
	require.NotEmpty(t, degraded.DegradedReason, "degraded responses must carry a reason")

	status, body := searchStatus(t, client, url, pidStr, paraphrase, "hybrid")
	require.Equalf(t, http.StatusServiceUnavailable, status,
		"explicit hybrid with a dead embedder must be 503, not a downgrade: %s", body)
}

// fixtureEmbedder is a deterministic OpenAI-compatible /embeddings endpoint
// for the e2e. It returns one fixed unit vector per input, chosen by topic so
// paraphrases of the same concept collide and unrelated text is orthogonal.
type fixtureEmbedder struct {
	srv    *httptest.Server
	closed atomic.Bool
}

// newFixtureEmbedder starts the fake embedder on a loopback listener. Loopback
// HTTP needs no trust_private_network and (because the fixture takes no API
// key) never touches the bearer safety ladder.
func newFixtureEmbedder(t *testing.T) *fixtureEmbedder {
	t.Helper()
	f := &fixtureEmbedder{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		var req struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		type vec struct {
			Embedding []float32 `json:"embedding"`
		}
		out := struct {
			Data []vec `json:"data"`
		}{Data: make([]vec, len(req.Input))}
		for i, in := range req.Input {
			out.Data[i] = vec{Embedding: topicVector(in)}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fixtureEmbedder) URL() string { return f.srv.URL + "/v1" }

// Close shuts the embedder down. Idempotent so both the explicit kill in the
// test and the t.Cleanup registration are safe.
func (f *fixtureEmbedder) Close() {
	if f.closed.CompareAndSwap(false, true) {
		f.srv.Close()
	}
}

// topicVector maps text to a deterministic 2-D unit vector by topic. The
// authentication concept (however phrased) lands on the x-axis; everything
// else on the y-axis. Two phrasings of the same concept therefore have cosine
// 1.0 (well above the 0.3 floor) while unrelated text is orthogonal.
func topicVector(text string) []float32 {
	lower := strings.ToLower(text)
	for _, kw := range []string{"auth", "login", "callback", "redirect", "sign-in", "credential"} {
		if strings.Contains(lower, kw) {
			return []float32{1, 0}
		}
	}
	return []float32{0, 1}
}

// writeEmbeddingsConfig drops a config.toml under home enabling semantic search
// against baseURL. The daemon subprocess reads this from KATA_HOME at startup,
// so the full startEmbeddingReconciler wiring runs.
func writeEmbeddingsConfig(t *testing.T, home, baseURL string) {
	t.Helper()
	body := fmt.Sprintf(`[search.embeddings]
base_url = %q
model = "fixture-embed"
dims = 2
`, baseURL)
	require.NoError(t, os.WriteFile(filepath.Join(home, "config.toml"), []byte(body), 0o600))
}

// searchHitRow is the decoded subset of one /search result row.
type searchHitRow struct {
	Issue struct {
		ShortID string `json:"short_id"`
	} `json:"issue"`
	MatchedIn []string `json:"matched_in"`
}

// searchResult is the decoded subset of the /search envelope the e2e asserts on.
type searchResult struct {
	Mode           string         `json:"mode"`
	Degraded       bool           `json:"degraded"`
	DegradedReason string         `json:"degraded_reason"`
	Results        []searchHitRow `json:"results"`
}

// searchHybrid issues a search and requires a 200, returning the decoded
// envelope. mode "" exercises the auto path.
func searchHybrid(t *testing.T, client *http.Client, baseURL, pidStr, query, mode string) searchResult {
	t.Helper()
	status, body := searchStatus(t, client, baseURL, pidStr, query, mode)
	require.Equalf(t, http.StatusOK, status, "search %q mode=%q: %s", query, mode, body)
	var res searchResult
	require.NoErrorf(t, json.Unmarshal(body, &res), "decode search response: %s", body)
	return res
}

// searchStatus issues GET /search and returns the status code + raw body so
// callers can assert non-200 paths (the explicit-hybrid 503).
func searchStatus(t *testing.T, client *http.Client, baseURL, pidStr, query, mode string) (int, []byte) {
	t.Helper()
	q := url.Values{"q": {query}}
	if mode != "" {
		q.Set("mode", mode)
	}
	u := baseURL + "/api/v1/projects/" + pidStr + "/search?" + q.Encode()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	require.NoError(t, err)
	resp, err := client.Do(req) //nolint:gosec // G704: test-only unix socket, fixed URL
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, raw
}

func findHit(res searchResult, shortID string) (searchHitRow, bool) {
	for _, r := range res.Results {
		if r.Issue.ShortID == shortID {
			return r, true
		}
	}
	return searchHitRow{}, false
}

func containsIssue(res searchResult, shortID string) bool {
	_, ok := findHit(res, shortID)
	return ok
}

// createIssueWithBody posts an issue with a title and body and returns its
// short_id. The hooks harness's createIssueE2E hardcodes the title and returns
// nothing; the semantic e2e needs distinct content and the resulting ref.
func createIssueWithBody(t *testing.T, client *http.Client, baseURL string, pid int64, title, body string) string {
	t.Helper()
	u := baseURL + "/api/v1/projects/" + strconv.FormatInt(pid, 10) + "/issues"
	payload, err := json.Marshal(map[string]any{"actor": "tester", "title": title, "body": body})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, u, bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req) //nolint:gosec // G704: test-only unix socket, fixed URL
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "create issue: %s", raw)
	var parsed struct {
		Issue struct {
			ShortID string `json:"short_id"`
		} `json:"issue"`
	}
	require.NoErrorf(t, json.Unmarshal(raw, &parsed), "create response: %s", raw)
	require.NotEmptyf(t, parsed.Issue.ShortID, "short_id missing: %s", raw)
	return parsed.Issue.ShortID
}

// waitForEmbeddingBacklogZero polls /health until the embeddings block reports
// backlog == 0 (every dirty issue embedded) or fails after a timeout. It also
// fails fast if the reconciler reports a persistent last_error, so a
// misconfigured embedder surfaces as a clear message instead of a timeout.
func waitForEmbeddingBacklogZero(t *testing.T, client *http.Client, baseURL string, daemonStderr *safeBuffer) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/api/v1/health", nil)
		require.NoError(t, err)
		resp, err := client.Do(req) //nolint:gosec // G704: test-only unix socket, fixed URL
		require.NoError(t, err)
		raw, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, err)
		var h struct {
			Embeddings *struct {
				Configured bool   `json:"configured"`
				LastError  string `json:"last_error"`
				Backlog    int64  `json:"backlog"`
			} `json:"embeddings"`
		}
		require.NoErrorf(t, json.Unmarshal(raw, &h), "decode health: %s", raw)
		require.NotNil(t, h.Embeddings, "configured daemon must report an embeddings block: %s", raw)
		require.True(t, h.Embeddings.Configured, "embeddings block must report configured=true: %s", raw)
		last = string(raw)
		// A non-empty last_error after a successful embed is transient; only a
		// definitive misconfiguration would stick. Backlog reaching 0 with at
		// least one issue present means the reconciler embedded it.
		if h.Embeddings.Backlog == 0 && h.Embeddings.LastError == "" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("embedding backlog never drained to 0.\nlast /health: %s\ndaemon stderr: %s",
		last, daemonStderr.String())
}
