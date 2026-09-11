package notify

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

var qqBotBindCommandRe = regexp.MustCompile(`(?i)^/?(绑定|bind)(?:\s*#?\s*(\d+))?$`)

type qqBotBindCommand struct {
	ID uint
}

type qqBotGroupAtEvent struct {
	ID          string `json:"id"`
	Content     string `json:"content"`
	GroupOpenID string `json:"group_openid"`
	GroupID     string `json:"group_id"`
}

type qqBotBindApplyResult struct {
	Updated []storage.NotificationChannel
	Reply   string
}

func parseQQBotBindCommand(content string) (qqBotBindCommand, bool) {
	m := qqBotBindCommandRe.FindStringSubmatch(strings.TrimSpace(content))
	if m == nil {
		return qqBotBindCommand{}, false
	}
	if m[2] == "" {
		return qqBotBindCommand{}, true
	}
	id, err := strconv.ParseUint(m[2], 10, 64)
	if err != nil || id == 0 {
		return qqBotBindCommand{}, false
	}
	return qqBotBindCommand{ID: uint(id)}, true
}

func parseQQBotChannelConfig(raw string) (qqBotConfig, bool) {
	var cfg qqBotConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return qqBotConfig{}, false
	}
	cfg.AppID = strings.TrimSpace(cfg.AppID)
	cfg.AppSecret = strings.TrimSpace(cfg.AppSecret)
	cfg.GroupOpenID = strings.TrimSpace(cfg.GroupOpenID)
	return cfg, cfg.AppID != ""
}

func applyQQBotBind(
	list []storage.NotificationChannel,
	decrypt func(string) (string, error),
	encrypt func(string) (string, error),
	appID string,
	sandbox bool,
	groupOpenID string,
	cmd qqBotBindCommand,
) qqBotBindApplyResult {
	appID = strings.TrimSpace(appID)
	groupOpenID = strings.TrimSpace(groupOpenID)
	if decrypt == nil || encrypt == nil || appID == "" || groupOpenID == "" {
		return qqBotBindApplyResult{Reply: "绑定失败：缺少机器人或群标识。"}
	}

	type item struct {
		ch  storage.NotificationChannel
		cfg qqBotConfig
	}
	matched := make([]item, 0)
	for i := range list {
		ch := list[i]
		if ch.Type != storage.NotifyQQBot || !ch.Enabled {
			continue
		}
		plain, err := decrypt(ch.ConfigCipher)
		if err != nil {
			continue
		}
		cfg, ok := parseQQBotChannelConfig(plain)
		if !ok || cfg.AppID != appID || cfg.Sandbox != sandbox {
			continue
		}
		matched = append(matched, item{ch: ch, cfg: cfg})
	}

	if cmd.ID != 0 {
		for _, it := range matched {
			if it.ch.ID != cmd.ID {
				continue
			}
			updated, err := setQQBotGroupOpenID(it.ch, it.cfg, groupOpenID, encrypt)
			if err != nil {
				return qqBotBindApplyResult{Reply: "绑定失败：无法保存渠道配置。"}
			}
			if updated == nil {
				return qqBotBindApplyResult{Reply: "这个群已经绑定过「" + it.ch.Name + "」。"}
			}
			return qqBotBindApplyResult{
				Updated: []storage.NotificationChannel{*updated},
				Reply:   "已绑定通知渠道「" + it.ch.Name + "」，以后告警会发到这个群。",
			}
		}
		return qqBotBindApplyResult{Reply: "找不到可绑定的渠道 #" + strconv.FormatUint(uint64(cmd.ID), 10) + "，请确认已保存并启用这个机器人。"}
	}

	var empty []item
	var already []item
	for _, it := range matched {
		if it.cfg.GroupOpenID == "" {
			empty = append(empty, it)
			continue
		}
		if it.cfg.GroupOpenID == groupOpenID {
			already = append(already, it)
		}
	}
	if len(empty) == 1 {
		updated, err := setQQBotGroupOpenID(empty[0].ch, empty[0].cfg, groupOpenID, encrypt)
		if err != nil {
			return qqBotBindApplyResult{Reply: "绑定失败：无法保存渠道配置。"}
		}
		if updated == nil {
			return qqBotBindApplyResult{Reply: "这个群已经绑定过「" + empty[0].ch.Name + "」。"}
		}
		return qqBotBindApplyResult{
			Updated: []storage.NotificationChannel{*updated},
			Reply:   "已绑定通知渠道「" + empty[0].ch.Name + "」，以后告警会发到这个群。",
		}
	}
	if len(empty) > 1 {
		var b strings.Builder
		b.WriteString("有多个未绑定渠道，请指定一个，例如：")
		for i, it := range empty {
			if i > 0 {
				b.WriteString("；")
			}
			b.WriteString("绑定#")
			b.WriteString(strconv.FormatUint(uint64(it.ch.ID), 10))
			b.WriteString("（")
			b.WriteString(it.ch.Name)
			b.WriteString("）")
		}
		return qqBotBindApplyResult{Reply: b.String()}
	}
	if len(already) > 0 {
		return qqBotBindApplyResult{Reply: "这个群已经绑定过「" + already[0].ch.Name + "」。"}
	}
	if len(matched) == 0 {
		return qqBotBindApplyResult{Reply: "没有找到可绑定的 QQ 渠道，请先在面板里保存并启用这个机器人。"}
	}
	return qqBotBindApplyResult{Reply: "这些渠道都已绑定其他群。改绑请发送 绑定#渠道ID。"}
}

func setQQBotGroupOpenID(ch storage.NotificationChannel, cfg qqBotConfig, groupOpenID string, encrypt func(string) (string, error)) (*storage.NotificationChannel, error) {
	if cfg.GroupOpenID == groupOpenID {
		return nil, nil
	}
	cfg.GroupOpenID = groupOpenID
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	cipherText, err := encrypt(string(raw))
	if err != nil {
		return nil, err
	}
	ch.ConfigCipher = cipherText
	return &ch, nil
}

func MergeQQBotConfigJSON(prev, next string) string {
	oldCfg, oldOK := parseQQBotChannelConfig(prev)
	newCfg, newOK := parseQQBotChannelConfig(next)
	if !newOK {
		return next
	}
	if oldOK {
		if newCfg.AppID == "" {
			newCfg.AppID = oldCfg.AppID
		}
		if newCfg.AppSecret == "" {
			newCfg.AppSecret = oldCfg.AppSecret
		}
		if newCfg.GroupOpenID == "" {
			newCfg.GroupOpenID = oldCfg.GroupOpenID
		}
	}
	raw, err := json.Marshal(newCfg)
	if err != nil {
		return next
	}
	return string(raw)
}

func (h *QQBotKeepAliveHub) handleDispatch(ctx context.Context, api *qqBotAPI, eventType string, raw json.RawMessage) {
	if h == nil || api == nil || eventType != "GROUP_AT_MESSAGE_CREATE" {
		return
	}
	var ev qqBotGroupAtEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return
	}
	groupOpenID := strings.TrimSpace(ev.GroupOpenID)
	if groupOpenID == "" {
		groupOpenID = strings.TrimSpace(ev.GroupID)
	}
	if groupOpenID == "" {
		return
	}

	reply := h.replyForCommand(ctx, api, groupOpenID, ev.Content)
	if strings.TrimSpace(reply) == "" {
		return
	}
	if err := api.replyGroup(ctx, groupOpenID, ev.ID, TruncateQQBotReply(reply)); err != nil {
		h.logger().Error("qqbot command reply", "err", err, "group_openid", groupOpenID)
	}
}

func (h *QQBotKeepAliveHub) replyForCommand(ctx context.Context, api *qqBotAPI, groupOpenID, content string) string {
	cmd := ParseQQBotUserCommand(content)
	switch cmd.Kind {
	case QQBotCmdHelp:
		return QQBotHelpText()
	case QQBotCmdTest:
		return QQBotTestReply()
	case QQBotCmdUnknown:
		return QQBotUnknownCommandReply()
	case QQBotCmdReport:
		if h.inbox == nil {
			return "查询服务未就绪。"
		}
		return h.inbox.HandleQQBotReport(ctx, cmd.Report)
	case QQBotCmdBind:
		if h.repo == nil || h.cipher == nil {
			return "绑定失败：服务未就绪。"
		}
		list, err := h.repo.ListChannels()
		if err != nil {
			h.logger().Error("qqbot bind list channels", "err", err)
			return "绑定失败：无法读取渠道。"
		}
		result := applyQQBotBind(list, h.cipher.Decrypt, h.cipher.Encrypt, api.appID, api.sandbox, groupOpenID, cmd.Bind)
		for i := range result.Updated {
			if err := h.repo.UpdateChannel(&result.Updated[i]); err != nil {
				h.logger().Error("qqbot bind save channel", "channel", result.Updated[i].Name, "err", err)
				return "绑定失败：无法保存渠道配置。"
			}
			h.logger().Info("qqbot bind saved", "channel", result.Updated[i].Name, "group_openid", groupOpenID)
		}
		return result.Reply
	default:
		return QQBotUnknownCommandReply()
	}
}

func (a *qqBotAPI) replyGroup(ctx context.Context, groupOpenID, msgID, content string) error {
	token, err := a.accessToken(ctx)
	if err != nil {
		return err
	}
	body := map[string]any{
		"msg_type": 0,
		"content":  content,
	}
	if strings.TrimSpace(msgID) != "" {
		body["msg_id"] = msgID
	}
	resp, err := a.http.R().
		SetContext(ctx).
		SetHeader("Authorization", "QQBot "+token).
		SetHeader("X-Union-Appid", a.appID).
		SetBody(body).
		Post(a.base() + "/v2/groups/" + groupOpenID + "/messages")
	if err != nil {
		return err
	}
	if resp.IsError() {
		return qqBotAPIError("reply", resp.Status(), resp.Body())
	}
	if reason := qqBotBusinessError(resp.Body()); reason != "" {
		return errors.New("qqbot reply failed: " + reason)
	}
	return nil
}
