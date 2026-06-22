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
