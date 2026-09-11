package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const qqBotKeepAliveReconcileInterval = 15 * time.Second

type qqBotWSPayload struct {
	Op int             `json:"op"`
	S  *int            `json:"s"`
	T  string          `json:"t"`
	D  json.RawMessage `json:"d"`
}

type qqBotHello struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

type qqBotGateway struct {
	URL string `json:"url"`
}

type qqBotKeepAliveTarget struct {
	Key       string
	AppID     string
	AppSecret string
	Sandbox   bool
}

type qqBotSessionFunc func(ctx context.Context, api *qqBotAPI, log *slog.Logger) error

type qqBotKeepAliveRunner struct {
	target qqBotKeepAliveTarget
	cancel context.CancelFunc
}

// QQBotKeepAliveHub 按已启用的 QQ 机器人渠道维持 WebSocket。
// 同一个 AppID + 沙箱开关共用一条连接；不同机器人各自连接。
type QQBotKeepAliveHub struct {
	repo   *storage.Notifications
	cipher *crypto.Cipher
	inbox  QQBotInbox
	log    *slog.Logger
	run    qqBotSessionFunc

	mu      sync.Mutex
	parent  context.Context
	runners map[string]*qqBotKeepAliveRunner
}

func NewQQBotKeepAliveHub(repo *storage.Notifications, cipher *crypto.Cipher, log *slog.Logger) *QQBotKeepAliveHub {
	if log == nil {
		log = slog.Default()
	}
	return &QQBotKeepAliveHub{
		repo:    repo,
		cipher:  cipher,
		log:     log,
		runners: map[string]*qqBotKeepAliveRunner{},
	}
}

func (h *QQBotKeepAliveHub) SetInbox(inbox QQBotInbox) {
	if h == nil {
		return
	}
	h.inbox = inbox
}

func (h *QQBotKeepAliveHub) Start(ctx context.Context) {
	h.mu.Lock()
	h.parent = ctx
	h.mu.Unlock()
	h.Sync()
	go h.loop(ctx)
}

func (h *QQBotKeepAliveHub) loop(ctx context.Context) {
	ticker := time.NewTicker(qqBotKeepAliveReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			h.Stop()
			return
		case <-ticker.C:
			h.Sync()
		}
	}
}

func (h *QQBotKeepAliveHub) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for key, runner := range h.runners {
		runner.cancel()
		delete(h.runners, key)
	}
}

// Sync 按当前渠道配置增删 WebSocket。渠道保存后立刻调用，平时也定时对账。
func (h *QQBotKeepAliveHub) logger() *slog.Logger {
	if h != nil && h.log != nil {
		return h.log
	}
	return slog.Default()
}

func (h *QQBotKeepAliveHub) Sync() {
	if h == nil || h.repo == nil || h.cipher == nil {
		return
	}
	list, err := h.repo.ListChannels()
	if err != nil {
		h.logger().Error("qqbot keepalive list channels", "err", err)
		return
	}
	h.reconcile(collectQQBotKeepAliveTargets(list, h.cipher.Decrypt, h.logger()))
}

func (h *QQBotKeepAliveHub) reconcile(desired []qqBotKeepAliveTarget) {
	h.mu.Lock()
	defer h.mu.Unlock()
	parent := h.parent
	if parent == nil {
		parent = context.Background()
	}

	want := make(map[string]qqBotKeepAliveTarget, len(desired))
	for _, t := range desired {
		want[t.Key] = t
	}

	for key, runner := range h.runners {
		next, ok := want[key]
		if !ok || runner.target.AppSecret != next.AppSecret {
			h.logger().Info("qqbot keepalive stop", "app_id", runner.target.AppID, "sandbox", runner.target.Sandbox)
			runner.cancel()
			delete(h.runners, key)
		}
	}

	for key, t := range want {
		if _, ok := h.runners[key]; ok {
			continue
		}
		ctx, cancel := context.WithCancel(parent)
		h.runners[key] = &qqBotKeepAliveRunner{target: t, cancel: cancel}
		api := newQQBotAPI(t.AppID, t.AppSecret, t.Sandbox)
		session := h.run
		if session == nil {
			session = h.keepAliveSession
		}
		log := h.logger().With("app_id", t.AppID, "sandbox", t.Sandbox)
		h.logger().Info("qqbot keepalive start", "app_id", t.AppID, "sandbox", t.Sandbox)
		go runQQBotKeepAliveLoop(ctx, api, log, session)
	}
}

func collectQQBotKeepAliveTargets(list []storage.NotificationChannel, decrypt func(string) (string, error), log *slog.Logger) []qqBotKeepAliveTarget {
	if decrypt == nil {
		return nil
	}
	if log == nil {
		log = slog.Default()
	}
	out := make([]qqBotKeepAliveTarget, 0)
	seen := map[string]string{}
	for i := range list {
		ch := list[i]
		if !ch.Enabled || ch.Type != storage.NotifyQQBot {
			continue
		}
		plain, err := decrypt(ch.ConfigCipher)
		if err != nil {
			log.Error("qqbot keepalive decrypt config", "channel", ch.Name, "err", err)
			continue
		}
		var cfg qqBotConfig
		if err := json.Unmarshal([]byte(plain), &cfg); err != nil {
			log.Error("qqbot keepalive parse config", "channel", ch.Name, "err", err)
			continue
		}
		cfg.AppID = strings.TrimSpace(cfg.AppID)
		cfg.AppSecret = strings.TrimSpace(cfg.AppSecret)
		if cfg.AppID == "" || cfg.AppSecret == "" {
			continue
		}
		t := qqBotKeepAliveTarget{
			Key:       qqBotKeepAliveKey(cfg.AppID, cfg.Sandbox),
			AppID:     cfg.AppID,
			AppSecret: cfg.AppSecret,
			Sandbox:   cfg.Sandbox,
		}
		if prev, ok := seen[t.Key]; ok {
			if prev != t.AppSecret {
				log.Warn("qqbot keepalive secret mismatch, keep first channel", "app_id", t.AppID, "sandbox", t.Sandbox)
			}
			continue
		}
		seen[t.Key] = t.AppSecret
		out = append(out, t)
	}
	return out
}

func qqBotKeepAliveKey(appID string, sandbox bool) string {
	if sandbox {
		return appID + "|sandbox"
	}
	return appID + "|prod"
}

func (h *QQBotKeepAliveHub) keepAliveSession(ctx context.Context, api *qqBotAPI, log *slog.Logger) error {
	return runQQBotKeepAliveSession(ctx, api, log, h.handleDispatch)
}

func runQQBotKeepAliveLoop(ctx context.Context, api *qqBotAPI, log *slog.Logger, session qqBotSessionFunc) {
	if log == nil {
		log = slog.Default()
	}
	if session == nil {
		session = func(ctx context.Context, api *qqBotAPI, log *slog.Logger) error {
			return runQQBotKeepAliveSession(ctx, api, log, nil)
		}
	}
	backoff := time.Second
	for {
		err := session(ctx, api, log)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Error("qqbot keepalive session ended", "err", err, "retry_in", backoff)
		} else {
			log.Warn("qqbot keepalive session closed", "retry_in", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func runQQBotKeepAliveSession(ctx context.Context, api *qqBotAPI, log *slog.Logger, onEvent func(context.Context, *qqBotAPI, string, json.RawMessage)) error {
	if log == nil {
		log = slog.Default()
	}
	token, err := api.accessToken(ctx)
	if err != nil {
		return err
	}
	gatewayURL, err := api.gatewayURL(ctx, token)
	if err != nil {
		return err
	}

	conn, _, err := websocket.Dial(ctx, gatewayURL, nil)
	if err != nil {
		return fmt.Errorf("dial gateway: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")

	log.Info("qqbot keepalive connected", "sandbox", api.sandbox, "gateway", gatewayURL)

	var (
		heartbeat  = 40 * time.Second
		identified bool
		lastSeq    *int
	)

	for {
		var payload qqBotWSPayload
		if err := wsjson.Read(ctx, conn, &payload); err != nil {
			return err
		}
		if payload.S != nil {
			seq := *payload.S
			lastSeq = &seq
		}

		switch payload.Op {
		case 10: // Hello
			var hello qqBotHello
			if err := json.Unmarshal(payload.D, &hello); err == nil && hello.HeartbeatInterval > 0 {
				heartbeat = time.Duration(hello.HeartbeatInterval) * time.Millisecond
			}
			if err := wsjson.Write(ctx, conn, map[string]any{
				"op": 2,
				"d": map[string]any{
					"token":   "QQBot " + token,
					"intents": 1 << 25, // GROUP_AND_C2C_EVENT，用来打印 group_openid
					"shard":   []int{0, 1},
				},
			}); err != nil {
				return fmt.Errorf("identify: %w", err)
			}
			identified = true
			go qqBotHeartbeatLoop(ctx, conn, heartbeat, func() *int { return lastSeq })
			log.Info("qqbot keepalive identified", "heartbeat", heartbeat)
		case 7: // Reconnect
			return errors.New("server requested reconnect")
		case 9: // Invalid session
			return errors.New("invalid session")
		case 0:
			if payload.T == "READY" {
				log.Info("qqbot keepalive ready")
				break
			}
			groupOpenID := extractQQBotGroupOpenID(payload.D)
			log.Info("qqbot event", "event", payload.T, "group_openid", groupOpenID, "payload", truncateQQBotPayload(payload.D))
			if onEvent != nil {
				onEvent(ctx, api, payload.T, payload.D)
			}
		}
		if !identified && payload.Op != 10 {
			return fmt.Errorf("unexpected opcode before hello: %d", payload.Op)
		}
	}
}

func (a *qqBotAPI) gatewayURL(ctx context.Context, token string) (string, error) {
	resp, err := a.http.R().
		SetContext(ctx).
		SetHeader("Authorization", "QQBot "+token).
		SetHeader("X-Union-Appid", a.appID).
		Get(a.base() + "/gateway")
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", qqBotAPIError("gateway", resp.Status(), resp.Body())
	}
	var gw qqBotGateway
	if err := json.Unmarshal(resp.Body(), &gw); err != nil {
		return "", fmt.Errorf("qqbot gateway: decode: %w", err)
	}
	if strings.TrimSpace(gw.URL) == "" {
		return "", errors.New("qqbot gateway: empty url")
	}
	return gw.URL, nil
}

func qqBotHeartbeatLoop(ctx context.Context, conn *websocket.Conn, interval time.Duration, seq func() *int) {
	if interval <= 0 {
		interval = 40 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := wsjson.Write(ctx, conn, map[string]any{"op": 1, "d": seq()}); err != nil {
				return
			}
		}
	}
}

func extractQQBotGroupOpenID(raw json.RawMessage) string {
	var ev struct {
		GroupOpenID string `json:"group_openid"`
		GroupID     string `json:"group_id"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return ""
	}
	if ev.GroupOpenID != "" {
		return ev.GroupOpenID
	}
	return ev.GroupID
}

func truncateQQBotPayload(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if len(s) > 400 {
		return s[:400] + "..."
	}
	return s
}
