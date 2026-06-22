package sqlitestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"go.kenn.io/kata/internal/db"
)

// vectorToBytes serializes float32s little-endian. The schema CHECK requires
// length(vector_bytes) = dims * 4, so the caller must pass len(v) == dims. The
// inverse (bytesToVector) lands with SearchVector, its only consumer.
func vectorToBytes(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// UpsertIssueEmbedding inserts or replaces the embedding row for an issue. The
// conflict target is issue_id, so each issue holds at most one vector; a model
// swap overwrites the prior vector and fingerprint in place.
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
		d.vectorCache.invalidate(e.Fingerprint)
		return nil
	})
}

// ListEmbedTargets returns issues that are missing an embedding, embedded under
// a different fingerprint, or whose content_revision has moved since they were
// embedded. Soft-deleted issues are excluded. Results are ordered by issue id
// so the reconciler makes deterministic forward progress.
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
// project at the active fingerprint — the per-query cache freshness probe. The
// max is empty when no rows exist.
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
