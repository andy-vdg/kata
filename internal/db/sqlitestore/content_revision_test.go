package sqlitestore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/db"
	"go.kenn.io/kata/internal/db/sqlitestore"
)

// contentRev reads issues.content_revision for issueID directly.
func contentRev(ctx context.Context, t *testing.T, d *sqlitestore.Store, issueID int64) int64 {
	t.Helper()
	var cr int64
	require.NoError(t, d.QueryRowContext(ctx,
		`SELECT content_revision FROM issues WHERE id = ?`, issueID).Scan(&cr))
	return cr
}

func TestContentRevisionBumpsOnTitleBodyOnly(t *testing.T) {
	ctx := context.Background()
	d := openTestDB(t)
	proj := createProject(ctx, t, d, "spoke-project")
	iss, _, err := d.CreateIssue(ctx, db.CreateIssueParams{
		ProjectID: proj.ID, Title: "first", Body: "b", Author: "tester",
	})
	require.NoError(t, err)
	base := contentRev(ctx, t, d, iss.ID)

	// Title edit bumps.
	newTitle := "second"
	_, _, _, err = d.EditIssue(ctx, db.EditIssueParams{IssueID: iss.ID, Title: &newTitle, Actor: "tester"})
	require.NoError(t, err)
	afterTitle := contentRev(ctx, t, d, iss.ID)
	require.Equalf(t, base+1, afterTitle, "title edit must bump content_revision")

	// Body edit bumps.
	newBody := "c"
	_, _, _, err = d.EditIssue(ctx, db.EditIssueParams{IssueID: iss.ID, Body: &newBody, Actor: "tester"})
	require.NoError(t, err)
	afterBody := contentRev(ctx, t, d, iss.ID)
	require.Equalf(t, afterTitle+1, afterBody, "body edit must bump content_revision")

	// Re-applying the same title does NOT bump (no-op edit).
	_, _, _, err = d.EditIssue(ctx, db.EditIssueParams{IssueID: iss.ID, Title: &newTitle, Actor: "tester"})
	require.NoError(t, err)
	require.Equalf(t, afterBody, contentRev(ctx, t, d, iss.ID),
		"re-applying the same title must not bump content_revision")

	// Owner edit does NOT bump.
	owner := "alice"
	_, _, _, err = d.EditIssue(ctx, db.EditIssueParams{IssueID: iss.ID, Owner: &owner, Actor: "tester"})
	require.NoError(t, err)
	require.Equalf(t, afterBody, contentRev(ctx, t, d, iss.ID),
		"owner edit must not bump content_revision")

	// Comment does NOT bump.
	_, _, err = d.CreateComment(ctx, db.CreateCommentParams{IssueID: iss.ID, Body: "c", Author: "tester"})
	require.NoError(t, err)
	require.Equalf(t, afterBody, contentRev(ctx, t, d, iss.ID),
		"comment must not bump content_revision")
}

func TestContentRevisionBumpsFromEditIssueAtomic(t *testing.T) {
	ctx := context.Background()
	d := openTestDB(t)
	proj := createProject(ctx, t, d, "spoke-project")
	iss, _, err := d.CreateIssue(ctx, db.CreateIssueParams{
		ProjectID: proj.ID, Title: "first", Body: "b", Author: "tester",
	})
	require.NoError(t, err)
	base := contentRev(ctx, t, d, iss.ID)

	// Atomic title edit bumps.
	newTitle := "second"
	_, err = d.EditIssueAtomic(ctx, db.EditIssueAtomicParams{IssueID: iss.ID, Title: &newTitle, Actor: "tester"})
	require.NoError(t, err)
	afterTitle := contentRev(ctx, t, d, iss.ID)
	require.Equalf(t, base+1, afterTitle, "atomic title edit must bump content_revision")

	// Atomic priority-only edit does NOT bump.
	prio := int64(2)
	_, err = d.EditIssueAtomic(ctx, db.EditIssueAtomicParams{IssueID: iss.ID, SetPriority: &prio, Actor: "tester"})
	require.NoError(t, err)
	require.Equalf(t, afterTitle, contentRev(ctx, t, d, iss.ID),
		"atomic priority edit must not bump content_revision")
}
