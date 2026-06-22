package sqlitestore_test

import (
	"context"
	"testing"
)

func TestSchemaHasEmbeddingSurface(t *testing.T) {
	ctx := context.Background()
	d := openTestDB(t) // fresh in-memory/temp Store

	// issues.content_revision exists.
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
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for col, seen := range want {
		if !seen {
			t.Errorf("issue_embeddings missing column %q", col)
		}
	}
}
