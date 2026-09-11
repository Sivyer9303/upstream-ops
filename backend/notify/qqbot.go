package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/go-resty/resty/v2"
)

const (
	qqBotTokenURL         = "https://api.bot.qq.com/app/getAppAccessToken"
	qqBotAPIBase          = "https://api.bot.qq.com"
	qqBotSandboxAPIBase   = "https://sandbox.api.sgroup.qq.com"
	qqBotTokenRefreshSkew = 60 * time.Second
)

func init() {
	Register(storage.NotifyQQBot, func(raw string) (Notifier, error) { return newQQBot(raw) })
}

type qqBotConfig struct {
	AppID       string `json:"app_id"`
	AppSecret   string `json:"app_secret"`
	GroupOpenID string `json:"group_openid"`
	Sandbox     bool   `json:"sandbox,omitempty"`
}

type qqBotAPI struct {
	appID     string
	appSecret string
	sandbox   bool
	tokenURL  string
	apiBase   string
	http      *resty.Client

	mu       sync.Mutex
	token    string
	tokenExp time.Time
}

type qqBot struct {
	cfg qqBotConfig
	api *qqBotAPI
}

func newQQBot(raw string) (*qqBot, error) {
	var cfg qqBotConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, err
	}
	cfg.AppID = strings.TrimSpace(cfg.AppID)
	cfg.AppSecret = strings.TrimSpace(cfg.AppSecret)
	cfg.GroupOpenID = strings.TrimSpace(cfg.GroupOpenID)
	if cfg.AppID == "" {
		return nil, errors.New("qqbot app_id is required")
	}
	if cfg.AppSecret == "" {
		return nil, errors.New("qqbot app_secret is required")
	}
	if cfg.GroupOpenID == "" {
		return nil, errors.New("qqbot group_openid is required")
	}
	return &qqBot{cfg: cfg, api: newQQBotAPI(cfg.AppID, cfg.AppSecret, cfg.Sandbox)}, nil
}

func newQQBotAPI(appID, appSecret string, sandbox bool) *qqBotAPI {
	return &qqBotAPI{
		appID:     appID,
		appSecret: appSecret,
		sandbox:   sandbox,
		http:      resty.New(),
	}
}

func (a *qqBotAPI) SetProxy(proxyURL string) {
	if proxyURL != "" {
		a.http.SetProxy(proxyURL)
	}
}

func (a *qqBotAPI) base() string {
	if a.apiBase != "" {
		return strings.TrimRight(a.apiBase, "/")
	}
	if a.sandbox {
		return qqBotSandboxAPIBase
	}
	return qqBotAPIBase
}

func (a *qqBotAPI) tokenEndpoint() string {
	if a.tokenURL != "" {
		return a.tokenURL
	}
	return qqBotTokenURL
}

func (q *qqBot) Type() storage.NotificationChannelType { return storage.NotifyQQBot }

func (q *qqBot) SetProxy(proxyURL string) {
	q.api.SetProxy(proxyURL)
}

func (q *qqBot) Send(ctx context.Context, msg Message) error {
	token, err := q.api.accessToken(ctx)
	if err != nil {
		return err
	}
	content := strings.TrimSpace(msg.Subject)
	if body := strings.TrimSpace(msg.Body); body != "" {
		if content != "" {
			content += "\n"
		}
		content += body
	}
	if content == "" {
		return errors.New("qqbot message is empty")
	}

	endpoint := q.api.base() + "/v2/groups/" + q.cfg.GroupOpenID + "/messages"
	resp, err := q.api.http.R().
		SetContext(ctx).
		SetHeader("Authorization", "QQBot "+token).
		SetHeader("X-Union-Appid", q.cfg.AppID).
		SetBody(map[string]any{
			"msg_type": 0,
			"content":  content,
		}).
		Post(endpoint)
	if err != nil {
		return err
	}
	if resp.IsError() {
		return qqBotAPIError("send", resp.Status(), resp.Body())
	}
	if reason := qqBotBusinessError(resp.Body()); reason != "" {
		return errors.New("qqbot send failed: " + reason)
	}
	return nil
}

func (a *qqBotAPI) accessToken(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token != "" && time.Now().Before(a.tokenExp) {
		return a.token, nil
	}

	resp, err := a.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(map[string]string{
			"appId":        a.appID,
			"clientSecret": a.appSecret,
		}).
		Post(a.tokenEndpoint())
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", qqBotAPIError("token", resp.Status(), resp.Body())
	}

	var raw struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   any    `json:"expires_in"`
		Message     string `json:"message"`
		Msg         string `json:"msg"`
		Code        any    `json:"code"`
		ErrCode     any    `json:"err_code"`
	}
	if err := json.Unmarshal(resp.Body(), &raw); err != nil {
		return "", fmt.Errorf("qqbot token: decode: %w", err)
	}
	if raw.AccessToken == "" {
		reason := raw.Message
		if reason == "" {
			reason = raw.Msg
		}
		if reason == "" {
			reason = strings.TrimSpace(string(resp.Body()))
		}
		if reason == "" {
			reason = "empty access_token"
		}
		return "", errors.New("qqbot token: " + reason)
	}

	expires := parseQQBotExpiresIn(raw.ExpiresIn)
	if expires <= 0 {
		expires = 2 * time.Hour
	}
	if expires > qqBotTokenRefreshSkew {
		expires -= qqBotTokenRefreshSkew
	}
	a.token = raw.AccessToken
	a.tokenExp = time.Now().Add(expires)
	return a.token, nil
}

func parseQQBotExpiresIn(v any) time.Duration {
	switch n := v.(type) {
	case float64:
		return time.Duration(n) * time.Second
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0
		}
		return time.Duration(i) * time.Second
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64)
		if err != nil {
			return 0
		}
		return time.Duration(i) * time.Second
	default:
		return 0
	}
}

func qqBotAPIError(op, status string, body []byte) error {
	if reason := qqBotBusinessError(body); reason != "" {
		return errors.New("qqbot " + op + " failed: " + reason)
	}
	return errors.New("qqbot " + op + " returned " + status)
}

func qqBotBusinessError(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var raw struct {
		Message string `json:"message"`
		Msg     string `json:"msg"`
		Code    any    `json:"code"`
		ErrCode any    `json:"err_code"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return ""
	}
	if !qqBotIsErrorCode(raw.Code) && !qqBotIsErrorCode(raw.ErrCode) {
		return ""
	}
	reason := strings.TrimSpace(raw.Message)
	if reason == "" {
		reason = strings.TrimSpace(raw.Msg)
	}
	if reason == "" {
		reason = fmt.Sprintf("code %v", firstNonNil(raw.Code, raw.ErrCode))
	}
	return reason
}

func qqBotIsErrorCode(v any) bool {
	switch n := v.(type) {
	case nil:
		return false
	case float64:
		return n != 0
	case json.Number:
		i, err := n.Int64()
		return err == nil && i != 0
	case string:
		s := strings.TrimSpace(n)
		return s != "" && s != "0"
	default:
		return fmt.Sprint(n) != "0"
	}
}

func firstNonNil(values ...any) any {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}
