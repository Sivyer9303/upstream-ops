package notify

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestNewQQBotRequiresFields(t *testing.T) {
	t.Parallel()
	if _, err := newQQBot(`{"app_id":"a","app_secret":"b"}`); err == nil {
		t.Fatal("expected group_openid error")
	}
	got, err := newQQBot(`{"app_id":"a","app_secret":"b","group_openid":"g","sandbox":true}`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type() != storage.NotifyQQBot {
		t.Fatalf("type = %s", got.Type())
	}
	if !got.api.sandbox {
		t.Fatal("sandbox should be true")
	}
}

func TestQQBotSend(t *testing.T) {
	t.Parallel()
	var gotAuth, gotAppID, gotContent string
	var tokenHits, sendHits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/token":
			tokenHits++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "tok-1",
				"expires_in":   "7200",
			})
		case strings.HasPrefix(r.URL.Path, "/v2/groups/"):
			sendHits++
			gotAuth = r.Header.Get("Authorization")
			gotAppID = r.Header.Get("X-Union-Appid")
			var body struct {
				MsgType int    `json:"msg_type"`
				Content string `json:"content"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode send body: %v", err)
			}
			if body.MsgType != 0 {
				t.Errorf("msg_type = %d", body.MsgType)
			}
			gotContent = body.Content
			_, _ = w.Write([]byte(`{"id":"m1"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	bot, err := newQQBot(`{"app_id":"app-1","app_secret":"sec","group_openid":"grp-1"}`)
	if err != nil {
		t.Fatal(err)
	}
	bot.api.tokenURL = srv.URL + "/token"
	bot.api.apiBase = srv.URL

	if err := bot.Send(context.Background(), Message{Subject: "余额告警", Body: "渠道 A 不足"}); err != nil {
		t.Fatal(err)
	}
	if err := bot.Send(context.Background(), Message{Subject: "第二次"}); err != nil {
		t.Fatal(err)
	}
	if tokenHits != 1 {
		t.Fatalf("tokenHits = %d, want 1", tokenHits)
	}
	if sendHits != 2 {
		t.Fatalf("sendHits = %d, want 2", sendHits)
	}
	if gotAuth != "QQBot tok-1" {
		t.Fatalf("authorization = %q", gotAuth)
	}
	if gotAppID != "app-1" {
		t.Fatalf("appid header = %q", gotAppID)
	}
	if !strings.Contains(gotContent, "第二次") {
		t.Fatalf("content = %q", gotContent)
	}
}

func TestQQBotSendReportsAPIError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok"})
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":40034105,"message":"主动消息发送失败，无权限"}`))
	}))
	t.Cleanup(srv.Close)

	bot, err := newQQBot(`{"app_id":"a","app_secret":"b","group_openid":"g"}`)
	if err != nil {
		t.Fatal(err)
	}
	bot.api.tokenURL = srv.URL + "/token"
	bot.api.apiBase = srv.URL

	err = bot.Send(context.Background(), Message{Subject: "x"})
	if err == nil || !strings.Contains(err.Error(), "无权限") {
		t.Fatalf("err = %v", err)
	}
}

func TestCollectQQBotKeepAliveTargets(t *testing.T) {
	t.Parallel()
	list := []storage.NotificationChannel{
		{Name: "off", Type: storage.NotifyQQBot, Enabled: false, ConfigCipher: `{"app_id":"a","app_secret":"s"}`},
		{Name: "tg", Type: storage.NotifyTelegram, Enabled: true, ConfigCipher: `{}`},
		{Name: "g1", Type: storage.NotifyQQBot, Enabled: true, ConfigCipher: `{"app_id":"bot-a","app_secret":"sec-a","group_openid":"g1"}`},
		{Name: "g2", Type: storage.NotifyQQBot, Enabled: true, ConfigCipher: `{"app_id":"bot-a","app_secret":"sec-a","group_openid":"g2"}`},
		{Name: "sandbox", Type: storage.NotifyQQBot, Enabled: true, ConfigCipher: `{"app_id":"bot-a","app_secret":"sec-a","sandbox":true}`},
		{Name: "bot-b", Type: storage.NotifyQQBot, Enabled: true, ConfigCipher: `{"app_id":"bot-b","app_secret":"sec-b"}`},
		{Name: "incomplete", Type: storage.NotifyQQBot, Enabled: true, ConfigCipher: `{"app_id":"x"}`},
	}
	got := collectQQBotKeepAliveTargets(list, func(s string) (string, error) { return s, nil }, nil)
	if len(got) != 3 {
		t.Fatalf("targets = %#v", got)
	}
	keys := map[string]string{}
	for _, tgot := range got {
		keys[tgot.Key] = tgot.AppSecret
	}
	if keys["bot-a|prod"] != "sec-a" || keys["bot-a|sandbox"] != "sec-a" || keys["bot-b|prod"] != "sec-b" {
		t.Fatalf("keys = %#v", keys)
	}
}

func TestQQBotKeepAliveHubReconciles(t *testing.T) {
	started := make(chan string, 8)
	stopped := make(chan string, 8)
	hub := &QQBotKeepAliveHub{
		log:     nil,
		runners: map[string]*qqBotKeepAliveRunner{},
		parent:  context.Background(),
		run: func(ctx context.Context, api *qqBotAPI, _ *slog.Logger) error {
			key := qqBotKeepAliveKey(api.appID, api.sandbox) + ":" + api.appSecret
			started <- key
			<-ctx.Done()
			stopped <- key
			return ctx.Err()
		},
	}

	hub.reconcile([]qqBotKeepAliveTarget{{
		Key: "bot-a|prod", AppID: "bot-a", AppSecret: "s1",
	}})
	if got := waitQQBotKey(t, started); got != "bot-a|prod:s1" {
		t.Fatalf("first start = %s", got)
	}

	hub.reconcile([]qqBotKeepAliveTarget{
		{Key: "bot-a|prod", AppID: "bot-a", AppSecret: "s2"},
		{Key: "bot-b|prod", AppID: "bot-b", AppSecret: "sb"},
	})
	if got := waitQQBotKey(t, stopped); got != "bot-a|prod:s1" {
		t.Fatalf("restart stop = %s", got)
	}
	got := map[string]bool{waitQQBotKey(t, started): true, waitQQBotKey(t, started): true}
	if !got["bot-a|prod:s2"] || !got["bot-b|prod:sb"] {
		t.Fatalf("after update started = %#v", got)
	}

	hub.reconcile(nil)
	gotStop := map[string]bool{waitQQBotKey(t, stopped): true, waitQQBotKey(t, stopped): true}
	if !gotStop["bot-a|prod:s2"] || !gotStop["bot-b|prod:sb"] {
		t.Fatalf("final stop = %#v", gotStop)
	}
}

func waitQQBotKey(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for keepalive session")
		return ""
	}
}

func TestParseQQBotExpiresIn(t *testing.T) {
	t.Parallel()
	if got := parseQQBotExpiresIn("7200"); got != 7200*time.Second {
		t.Fatalf("string expires = %s", got)
	}
	if got := parseQQBotExpiresIn(float64(3600)); got != time.Hour {
		t.Fatalf("float expires = %s", got)
	}
}

func TestQQBotKeepAliveSessionIdentifies(t *testing.T) {
	t.Parallel()
	identified := make(chan struct{}, 1)
	ws := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer c.CloseNow()
		ctx := r.Context()
		if err := wsjson.Write(ctx, c, map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 60000},
		}); err != nil {
			return
		}
		var identify struct {
			Op int `json:"op"`
			D  struct {
				Token   string `json:"token"`
				Intents int    `json:"intents"`
			} `json:"d"`
		}
		if err := wsjson.Read(ctx, c, &identify); err != nil {
			t.Errorf("read identify: %v", err)
			return
		}
		if identify.Op != 2 || identify.D.Token != "QQBot tok-ws" || identify.D.Intents != 1<<25 {
			t.Errorf("identify = %+v", identify)
			return
		}
		select {
		case identified <- struct{}{}:
		default:
		}
		_ = wsjson.Write(ctx, c, map[string]any{"op": 0, "t": "READY", "d": map[string]any{}})
		<-ctx.Done()
	}))
	t.Cleanup(ws.Close)

	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok-ws", "expires_in": 7200})
		case "/gateway":
			_ = json.NewEncoder(w).Encode(map[string]any{"url": "ws://" + strings.TrimPrefix(ws.URL, "http://")})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(httpSrv.Close)

	api := newQQBotAPI("app", "sec", true)
	api.tokenURL = httpSrv.URL + "/token"
	api.apiBase = httpSrv.URL

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- runQQBotKeepAliveSession(ctx, api, nil, nil)
	}()

	select {
	case <-identified:
		cancel()
	case err := <-errCh:
		t.Fatalf("session ended early: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for identify")
	}
}

func TestParseQQBotBindCommand(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in string
		ok bool
		id uint
	}{
		{"绑定", true, 0},
		{" /bind ", true, 0},
		{"绑定#12", true, 12},
		{"bind #3", true, 3},
		{"绑定好了", false, 0},
		{"hello", false, 0},
	}
	for _, tc := range cases {
		cmd, ok := parseQQBotBindCommand(tc.in)
		if ok != tc.ok || cmd.ID != tc.id {
			t.Fatalf("%q => ok=%v id=%d, want ok=%v id=%d", tc.in, ok, cmd.ID, tc.ok, tc.id)
		}
	}
}

func TestApplyQQBotBindFillsEmptyChannel(t *testing.T) {
	t.Parallel()
	list := []storage.NotificationChannel{
		{ID: 2, Name: "QQ群", Type: storage.NotifyQQBot, Enabled: true, ConfigCipher: `{"app_id":"bot","app_secret":"s"}`},
	}
	got := applyQQBotBind(list, identCrypt, identCrypt, "bot", false, "GRP1", qqBotBindCommand{})
	if len(got.Updated) != 1 || !strings.Contains(got.Reply, "已绑定") {
		t.Fatalf("%#v", got)
	}
	cfg, ok := parseQQBotChannelConfig(got.Updated[0].ConfigCipher)
	if !ok || cfg.GroupOpenID != "GRP1" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestApplyQQBotBindRequiresChannelIDWhenMultipleEmpty(t *testing.T) {
	t.Parallel()
	list := []storage.NotificationChannel{
		{ID: 2, Name: "A", Type: storage.NotifyQQBot, Enabled: true, ConfigCipher: `{"app_id":"bot","app_secret":"s"}`},
		{ID: 3, Name: "B", Type: storage.NotifyQQBot, Enabled: true, ConfigCipher: `{"app_id":"bot","app_secret":"s"}`},
	}
	auto := applyQQBotBind(list, identCrypt, identCrypt, "bot", false, "GRP1", qqBotBindCommand{})
	if len(auto.Updated) != 0 || !strings.Contains(auto.Reply, "绑定#") {
		t.Fatalf("auto = %#v", auto)
	}
	picked := applyQQBotBind(list, identCrypt, identCrypt, "bot", false, "GRP1", qqBotBindCommand{ID: 3})
	if len(picked.Updated) != 1 || picked.Updated[0].ID != 3 {
		t.Fatalf("picked = %#v", picked)
	}
}

func TestMergeQQBotConfigJSONKeepsOpenID(t *testing.T) {
	t.Parallel()
	got := MergeQQBotConfigJSON(
		`{"app_id":"a","app_secret":"old","group_openid":"G1"}`,
		`{"app_id":"a","app_secret":"new","group_openid":""}`,
	)
	cfg, ok := parseQQBotChannelConfig(got)
	if !ok || cfg.AppSecret != "new" || cfg.GroupOpenID != "G1" {
		t.Fatalf("merged = %s", got)
	}
}

func identCrypt(s string) (string, error) { return s, nil }

func TestParseQQBotUserCommand(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in     string
		kind   string
		report string
	}{
		{"", QQBotCmdHelp, ""},
		{"帮助", QQBotCmdHelp, ""},
		{" /help ", QQBotCmdHelp, ""},
		{"测试", QQBotCmdTest, ""},
		{"余额", QQBotCmdReport, QQBotReportBalanceDigest},
		{"余额不足", QQBotCmdReport, QQBotReportBalanceLow},
		{"倍率变化", QQBotCmdReport, QQBotReportRateChanged},
		{"分组", QQBotCmdReport, QQBotReportRateGroup},
		{"公告", QQBotCmdReport, QQBotReportAnnouncement},
		{"订阅", QQBotCmdReport, QQBotReportSubscription},
		{"绑定#4", QQBotCmdBind, ""},
		{"乱写", QQBotCmdUnknown, ""},
	}
	for _, tc := range cases {
		got := ParseQQBotUserCommand(tc.in)
		if got.Kind != tc.kind || got.Report != tc.report {
			t.Fatalf("%q => %+v, want kind=%s report=%s", tc.in, got, tc.kind, tc.report)
		}
	}
}

func TestQQBotHelpTextListsReports(t *testing.T) {
	t.Parallel()
	text := QQBotHelpText()
	for _, word := range []string{"帮助", "绑定", "余额汇总", "上游公告", "订阅通知"} {
		if !strings.Contains(text, word) {
			t.Fatalf("help missing %q:\n%s", word, text)
		}
	}
}

func TestReplyForCommandHelpAndUnknown(t *testing.T) {
	t.Parallel()
	h := &QQBotKeepAliveHub{}
	if !strings.Contains(h.replyForCommand(context.Background(), &qqBotAPI{}, "g", "帮助"), "口令") {
		t.Fatal("help reply")
	}
	if !strings.Contains(h.replyForCommand(context.Background(), &qqBotAPI{}, "g", "abc"), "帮助") {
		t.Fatal("unknown reply")
	}
	if h.replyForCommand(context.Background(), &qqBotAPI{}, "g", "测试") != QQBotTestReply() {
		t.Fatal("test reply")
	}
}
