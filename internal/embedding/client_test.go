package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newFakeServer(t *testing.T, status int, body string, retryAfter string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func TestEmbedNormalizesVectors(t *testing.T) {
	srv := newFakeServer(t, 200, `{"data":[{"embedding":[3,4]}]}`, "")
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL, Model: "m", Dims: 2})
	if err != nil {
		t.Fatal(err)
	}
	vecs, err := c.Embed(context.Background(), []string{"hello"})
	if err != nil {
		t.Fatal(err)
	}
	// [3,4] normalized is [0.6,0.8].
	if math.Abs(float64(vecs[0][0])-0.6) > 1e-6 || math.Abs(float64(vecs[0][1])-0.8) > 1e-6 {
		t.Fatalf("not normalized: %v", vecs[0])
	}
}

func TestEmbedDimsMismatchIsDefinitive(t *testing.T) {
	srv := newFakeServer(t, 200, `{"data":[{"embedding":[1,2,3]}]}`, "")
	defer srv.Close()
	c, _ := New(Config{BaseURL: srv.URL, Model: "m", Dims: 2})
	_, err := c.Embed(context.Background(), []string{"x"})
	if err == nil {
		t.Fatal("expected dims-mismatch error")
	}
}

func TestEmbed401IsDefinitive(t *testing.T) {
	srv := newFakeServer(t, 401, `{"error":"bad key"}`, "")
	defer srv.Close()
	c, _ := New(Config{BaseURL: srv.URL, Model: "m", Dims: 2})
	_, err := c.Embed(context.Background(), []string{"x"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || !apiErr.Definitive() {
		t.Fatalf("want definitive APIError, got %v", err)
	}
}

func TestEmbed429CarriesRetryAfter(t *testing.T) {
	srv := newFakeServer(t, 429, `{}`, "7")
	defer srv.Close()
	c, _ := New(Config{BaseURL: srv.URL, Model: "m", Dims: 2})
	_, err := c.Embed(context.Background(), []string{"x"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want APIError, got %v", err)
	}
	if apiErr.Definitive() {
		t.Fatal("429 must not be definitive")
	}
	if apiErr.RetryAfter != 7*time.Second {
		t.Fatalf("RetryAfter = %v, want 7s", apiErr.RetryAfter)
	}
}

func TestEmbedKeyOnlyToConfiguredOrigin(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"embedding": []float32{1, 0}}}})
	}))
	defer srv.Close()
	c, _ := New(Config{BaseURL: srv.URL, Model: "m", Dims: 2, APIKey: "secret"})
	if _, err := c.Embed(context.Background(), []string{"x"}); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer secret" {
		t.Fatalf("auth header = %q", gotAuth)
	}
}
