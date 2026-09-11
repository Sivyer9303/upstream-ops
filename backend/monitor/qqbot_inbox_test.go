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

func TestFormatQQBotRateCatalog(t *testing.T) {
	t.Parallel()
	text := formatQQBotRateCatalog([]storage.Channel{
		{ID: 3, Name: "NewAPI"},
		{ID: 8, Name: "备用站"},
	}, map[uint]int{3: 12, 8: 80})
	for _, word := range []string{"NewAPI", "备用站", "#倍率#3", "#倍率#8", "12 条", "80 条"} {
		if !strings.Contains(text, word) {
			t.Fatalf("catalog missing %q:\n%s", word, text)
		}
	}
}

func TestMatchQQBotRateChannel(t *testing.T) {
	t.Parallel()
	list := []storage.Channel{
		{ID: 3, Name: "NewAPI 主站"},
		{ID: 8, Name: "备用站"},
	}
	got, errMsg := matchQQBotRateChannel(list, "3")
	if errMsg != "" || got.ID != 3 {
		t.Fatalf("id match: %+v %s", got, errMsg)
	}
	got, errMsg = matchQQBotRateChannel(list, "备用")
	if errMsg != "" || got.ID != 8 {
		t.Fatalf("name match: %+v %s", got, errMsg)
	}
	if _, errMsg = matchQQBotRateChannel(list, "99"); errMsg == "" {
		t.Fatal("missing id should error")
	}
}

func TestFormatQQBotRateDetail(t *testing.T) {
	t.Parallel()
	ch := storage.Channel{ID: 3, Name: "NewAPI"}
	text := formatQQBotRateDetail(ch, []storage.RateSnapshot{
		{ModelName: "gpt-4o", Ratio: 1, CompletionRatio: 2},
		{ModelName: "claude-3", Ratio: 1.25},
	}, "")
	if !strings.Contains(text, "gpt-4o  1.0000 / 补全 2.0000") || !strings.Contains(text, "claude-3  1.2500") {
		t.Fatalf("detail =\n%s", text)
	}
	filtered := formatQQBotRateDetail(ch, []storage.RateSnapshot{
		{ModelName: "gpt-4o", Ratio: 1},
		{ModelName: "claude-3", Ratio: 1.25},
	}, "gpt")
	if !strings.Contains(filtered, "gpt-4o") || strings.Contains(filtered, "claude") {
		t.Fatalf("filter =\n%s", filtered)
	}
}
