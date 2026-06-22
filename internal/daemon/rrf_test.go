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
		{"hybrid", false, modeLexical, true},   // 400
		{"semantic", false, modeLexical, true}, // 400
		{"hybrid", true, modeHybrid, false},
		{"semantic", true, modeSemantic, false},
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
