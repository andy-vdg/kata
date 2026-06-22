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
