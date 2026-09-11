package monitor

import (
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestFormatBalanceDigest(t *testing.T) {
	t.Parallel()
	bal := 1.25
	low := 0.2
	subject, body := formatBalanceDigest([]storage.Channel{
		{Name: "A", LastBalance: &bal, BalanceThreshold: 1},
		{Name: "B", LastBalance: &low, BalanceThreshold: 1, LastError: ""},
	})
	if !strings.Contains(subject, "2 渠道") || !strings.Contains(body, "A") || !strings.Contains(body, "偏低") {
		t.Fatalf("subject=%s body=%s", subject, body)
	}
}

func TestClipLine(t *testing.T) {
	t.Parallel()
	if got := clipLine("hello world", 5); got != "hell…" {
		t.Fatalf("got %q", got)
	}
}
