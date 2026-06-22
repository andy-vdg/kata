package embedding

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestEmbedTextJoinsAndTruncatesOnRuneBoundary(t *testing.T) {
	got := EmbedText("Title", "Body")
	if got != "Title\n\nBody" {
		t.Fatalf("join: got %q", got)
	}
	// Use a 3-byte rune so a byte-based truncation (s[:maxEmbedChars]) would
	// slice mid-rune and corrupt UTF-8. The recipe must cut on a rune boundary.
	// "三" is 3 bytes; maxEmbedChars is not a multiple of 3, so a byte cut lands
	// inside a rune.
	long := strings.Repeat("三", maxEmbedChars+100)
	out := EmbedText(long, "")
	if !utf8.ValidString(out) {
		t.Fatalf("truncation split a multi-byte rune: output is not valid UTF-8")
	}
	if n := utf8.RuneCountInString(out); n != maxEmbedChars {
		t.Fatalf("truncation kept %d runes, want exactly %d", n, maxEmbedChars)
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
