package sqlitestore_test

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/db/sqlitestore"
)

// repeat63 is 63 zero characters; prefixing one distinguishing character makes
// a 64-char fingerprint that satisfies the schema's length(embed_fingerprint)=64
// CHECK.
const repeat63 = "000000000000000000000000000000000000000000000000000000000000000"

// mkEmbeddingIssue creates an issue with a non-empty title and body so the
// embed-target text columns are populated.
func mkEmbeddingIssue(ctx context.Context, t *testing.T, d *sqlitestore.Store, projID int64, title string) db.Issue {
	t.Helper()
	iss, _, err := d.CreateIssue(ctx, db.CreateIssueParams{
		ProjectID: projID, Title: title, Body: "body", Author: "tester",
	})
	require.NoError(t, err)
	return iss
}

func TestListEmbedTargetsAndUpsert(t *testing.T) {
	ctx := context.Background()
	d := openTestDB(t)
	proj := createProject(ctx, t, d, "spoke-project")
	iss := mkEmbeddingIssue(ctx, t, d, proj.ID, "needs embedding")
	fp := "a" + repeat63

	// Initially: the issue is a target (no embedding row yet).
	targets, err := d.ListEmbedTargets(ctx, fp, 10)
	require.NoError(t, err)
	require.Lenf(t, targets, 1, "expected the fresh issue to be a target")
	require.Equal(t, iss.ID, targets[0].IssueID)
	require.Equal(t, "needs embedding", targets[0].Title)
	require.Equal(t, "body", targets[0].Body)

	// Upsert an embedding at the issue's current content_revision.
	require.NoError(t, d.UpsertIssueEmbedding(ctx, db.IssueEmbedding{
		IssueID: iss.ID, EmbeddedContentRevision: targets[0].ContentRevision,
		Fingerprint: fp, Dims: 2, Vector: []float32{1, 0},
	}))

	// Now it is not a target.
	targets, err = d.ListEmbedTargets(ctx, fp, 10)
	require.NoError(t, err)
	require.Emptyf(t, targets, "expected no targets after upsert")

	// Editing the title makes it a target again (content_revision moved).
	nt := "edited"
	_, _, _, err = d.EditIssue(ctx, db.EditIssueParams{IssueID: iss.ID, Title: &nt, Actor: "tester"})
	require.NoError(t, err)
	targets, err = d.ListEmbedTargets(ctx, fp, 10)
	require.NoError(t, err)
	require.Lenf(t, targets, 1, "expected the stale issue to be a target after a title edit")
	require.Equal(t, "edited", targets[0].Title)

	// A different fingerprint sees the issue as a target (model swap).
	targets, err = d.ListEmbedTargets(ctx, "b"+repeat63, 10)
	require.NoError(t, err)
	require.Lenf(t, targets, 1, "a fingerprint swap must re-target every issue")
}

func TestUpsertIssueEmbeddingReplacesRowAndRoundTripsVector(t *testing.T) {
	ctx := context.Background()
	d := openTestDB(t)
	proj := createProject(ctx, t, d, "spoke-project")
	iss := mkEmbeddingIssue(ctx, t, d, proj.ID, "vec")
	fp := "a" + repeat63

	readVector := func() (int, []float32) {
		t.Helper()
		var dims int
		var raw []byte
		require.NoError(t, d.QueryRowContext(ctx,
			`SELECT dims, vector_bytes FROM issue_embeddings WHERE issue_id = ?`, iss.ID).
			Scan(&dims, &raw))
		out := make([]float32, len(raw)/4)
		for i := range out {
			out[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
		}
		return dims, out
	}

	first := []float32{0.6, 0.8}
	require.NoError(t, d.UpsertIssueEmbedding(ctx, db.IssueEmbedding{
		IssueID: iss.ID, EmbeddedContentRevision: 0, Fingerprint: fp, Dims: 2, Vector: first,
	}))
	gotDims, gotVec := readVector()
	require.Equal(t, 2, gotDims)
	require.InDelta(t, float64(first[0]), float64(gotVec[0]), 1e-6)
	require.InDelta(t, float64(first[1]), float64(gotVec[1]), 1e-6)

	// The conflict target is issue_id, so a second upsert replaces the row,
	// including dims and the vector blob.
	second := []float32{1, 0, 0}
	require.NoError(t, d.UpsertIssueEmbedding(ctx, db.IssueEmbedding{
		IssueID: iss.ID, EmbeddedContentRevision: 0, Fingerprint: fp, Dims: 3, Vector: second,
	}))
	gotDims, gotVec = readVector()
	require.Equal(t, 3, gotDims)
	require.Len(t, gotVec, 3)
	require.InDelta(t, 1.0, float64(gotVec[0]), 1e-6)

	// Exactly one row per issue.
	var rows int
	require.NoError(t, d.QueryRowContext(ctx,
		`SELECT count(*) FROM issue_embeddings WHERE issue_id = ?`, iss.ID).Scan(&rows))
	require.Equal(t, 1, rows)
}

func TestListEmbedTargetsExcludesSoftDeleted(t *testing.T) {
	ctx := context.Background()
	d := openTestDB(t)
	proj := createProject(ctx, t, d, "spoke-project")
	live := mkEmbeddingIssue(ctx, t, d, proj.ID, "live")
	gone := mkEmbeddingIssue(ctx, t, d, proj.ID, "gone")
	fp := "a" + repeat63

	_, _, _, err := d.SoftDeleteIssue(ctx, gone.ID, "tester")
	require.NoError(t, err)

	targets, err := d.ListEmbedTargets(ctx, fp, 10)
	require.NoError(t, err)
	require.Lenf(t, targets, 1, "soft-deleted issues must not be embed targets")
	require.Equal(t, live.ID, targets[0].IssueID)
}

func TestListEmbedTargetsRespectsLimit(t *testing.T) {
	ctx := context.Background()
	d := openTestDB(t)
	proj := createProject(ctx, t, d, "spoke-project")
	for i := 0; i < 5; i++ {
		mkEmbeddingIssue(ctx, t, d, proj.ID, "issue")
	}
	fp := "a" + repeat63

	targets, err := d.ListEmbedTargets(ctx, fp, 2)
	require.NoError(t, err)
	require.Len(t, targets, 2)
}

func TestSearchVectorRanksAndRespectsVisibility(t *testing.T) {
	ctx := context.Background()
	d := openTestDB(t)
	proj := createProject(ctx, t, d, "spoke-project")
	fp := "a" + repeat63

	near := mkEmbeddingIssue(ctx, t, d, proj.ID, "near")
	far := mkEmbeddingIssue(ctx, t, d, proj.ID, "far")
	embed := func(iss db.Issue, v []float32) {
		cr := contentRev(ctx, t, d, iss.ID)
		require.NoError(t, d.UpsertIssueEmbedding(ctx, db.IssueEmbedding{
			IssueID: iss.ID, EmbeddedContentRevision: cr, Fingerprint: fp, Dims: 2, Vector: v,
		}))
	}
	embed(near, []float32{1, 0})
	embed(far, []float32{0, 1})

	// Query close to "near".
	hits, err := d.SearchVector(ctx, proj.ID, []float32{1, 0}, fp, 10, false)
	require.NoError(t, err)
	require.Lenf(t, hits, 2, "both embedded issues should rank")
	require.Equalf(t, near.ID, hits[0].Issue.ID, "the nearer vector must rank first")
	require.Greaterf(t, hits[0].Score, hits[1].Score, "similarity must be strictly descending")
	for _, h := range hits {
		require.Equalf(t, []string{"semantic"}, h.MatchedIn, "matched_in must be the semantic leg")
	}

	// Soft-delete "near": it must drop out (visibility resolved live).
	_, _, _, err = d.SoftDeleteIssue(ctx, near.ID, "tester")
	require.NoError(t, err)
	hits, err = d.SearchVector(ctx, proj.ID, []float32{1, 0}, fp, 10, false)
	require.NoError(t, err)
	require.Lenf(t, hits, 1, "a soft-deleted issue must not surface")
	require.Equal(t, far.ID, hits[0].Issue.ID)

	// A wrong-fingerprint query returns nothing (no cross-model compare).
	hits, err = d.SearchVector(ctx, proj.ID, []float32{1, 0}, "b"+repeat63, 10, false)
	require.NoError(t, err)
	require.Emptyf(t, hits, "a different fingerprint must not match any vector")
}

func TestEmbeddingStats(t *testing.T) {
	ctx := context.Background()
	d := openTestDB(t)
	proj := createProject(ctx, t, d, "spoke-project")
	other := createProject(ctx, t, d, "hub-project")
	fp := "a" + repeat63

	// No embeddings yet: zero count, empty maxUpdatedAt.
	count, maxUpdated, err := d.EmbeddingStats(ctx, proj.ID, fp)
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
	require.Empty(t, maxUpdated)

	a := mkEmbeddingIssue(ctx, t, d, proj.ID, "a")
	b := mkEmbeddingIssue(ctx, t, d, proj.ID, "b")
	otherIss := mkEmbeddingIssue(ctx, t, d, other.ID, "c")
	for _, iss := range []db.Issue{a, b, otherIss} {
		require.NoError(t, d.UpsertIssueEmbedding(ctx, db.IssueEmbedding{
			IssueID: iss.ID, EmbeddedContentRevision: 0, Fingerprint: fp, Dims: 2, Vector: []float32{1, 0},
		}))
	}

	// Stats are scoped to the project and the fingerprint.
	count, maxUpdated, err = d.EmbeddingStats(ctx, proj.ID, fp)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	require.NotEmpty(t, maxUpdated)

	// A different fingerprint sees nothing for this project.
	count, maxUpdated, err = d.EmbeddingStats(ctx, proj.ID, "b"+repeat63)
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
	require.Empty(t, maxUpdated)
}
