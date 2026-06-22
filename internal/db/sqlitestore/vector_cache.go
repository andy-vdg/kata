package sqlitestore

import "sync"

// vectorCache holds normalized vectors per (projectID, fingerprint) for the
// brute-force vector search leg. The entry map and lookup logic land with
// SearchVector in a follow-up; for now the type exists so the Store can hold it
// and write paths can signal invalidation.
type vectorCache struct {
	mu sync.Mutex
	// entries keyed by "<projectID>:<fingerprint>"; populated with SearchVector.
}

func newVectorCache() *vectorCache { return &vectorCache{} }

// invalidate drops any cached vectors for the given fingerprint. There is
// nothing cached yet, so this is a no-op until SearchVector lands; it exists so
// UpsertIssueEmbedding's call site is correct now.
func (c *vectorCache) invalidate(fingerprint string) {
	_ = fingerprint
	c.mu.Lock()
	defer c.mu.Unlock()
}
