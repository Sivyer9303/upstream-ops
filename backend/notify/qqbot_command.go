package notify

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	QQBotCmdUnknown = "unknown"
	QQBotCmdHelp    = "help"
	QQBotCmdTest    = "test"
	QQBotCmdBind    = "bind"
	QQBotCmdReport  = "report"

	QQBotReportBalanceLow    = "balance_low"
	QQBotReportBalanceDigest = "balance_digest"
	QQBotReportRateChanged   = "rate_changed"
	QQBotReportRateGroup     = "rate_group_changed"
	QQBotReportAnnouncement  = "announcement"
	QQBotReportLoginFailed   = "login_failed"
	QQBotReportCaptchaFailed = "captcha_failed"
	QQBotReportMonitorFailed = "monitor_failed"
	QQBotReportSubscription  = "subscription_notice"

	qqBotReplyMaxRunes = 1800
)

// QQBotUserCommand 是群里 @ 机器人后的口令。
type QQBotUserCommand struct {
	Kind   string
	Report string
	Bind   qqBotBindCommand
}

// QQBotInbox 按口令生成当前状态回复。绑定仍由 Hub 自己处理。
type QQBotInbox interface {
	HandleQQBotReport(ctx context.Context, report string) string
}

type qqBotCommandSpec struct {
	Kind     string
	Report   string
	Title    string
	Summary  string
	Keywords []string
}

func qqBotCommandCatalog() []qqBotCommandSpec {
	return []qqBotCommandSpec{
		{Kind: QQBotCmdHelp, Title: "帮助", Summary: "查看全部口令", Keywords: []string{"帮助", "help", "?"}},
		{Kind: QQBotCmdBind, Title: "绑定", Summary: "把当前群绑到通知渠道，多个渠道用 绑定#ID", Keywords: []string{"绑定", "bind"}},
		{Kind: QQBotCmdTest, Title: "测试", Summary: "发送一条测试消息", Keywords: []string{"测试", "test"}},
		{Kind: QQBotCmdReport, Report: QQBotReportBalanceLow, Title: "余额不足", Summary: "当前低于阈值的渠道", Keywords: []string{"余额不足", "偏低"}},
		{Kind: QQBotCmdReport, Report: QQBotReportBalanceDigest, Title: "余额汇总", Summary: "已监控渠道余额", Keywords: []string{"余额汇总", "余额", "汇总"}},
		{Kind: QQBotCmdReport, Report: QQBotReportRateChanged, Title: "倍率变化", Summary: "最近倍率变动", Keywords: []string{"倍率变化", "倍率"}},
		{Kind: QQBotCmdReport, Report: QQBotReportRateGroup, Title: "分组变动", Summary: "最近分组增减", Keywords: []string{"分组变动", "分组"}},
		{Kind: QQBotCmdReport, Report: QQBotReportAnnouncement, Title: "上游公告", Summary: "最近同步的公告", Keywords: []string{"上游公告", "公告"}},
		{Kind: QQBotCmdReport, Report: QQBotReportLoginFailed, Title: "登录失败", Summary: "最近登录失败", Keywords: []string{"登录失败"}},
		{Kind: QQBotCmdReport, Report: QQBotReportCaptchaFailed, Title: "验证码失败", Summary: "打码平台异常", Keywords: []string{"验证码失败", "验证码"}},
		{Kind: QQBotCmdReport, Report: QQBotReportMonitorFailed, Title: "采集失败", Summary: "最近采集失败", Keywords: []string{"采集失败", "监控失败", "失败"}},
		{Kind: QQBotCmdReport, Report: QQBotReportSubscription, Title: "订阅通知", Summary: "订阅余量和到期", Keywords: []string{"订阅通知", "订阅"}},
	}
}

func ParseQQBotUserCommand(content string) QQBotUserCommand {
	raw := strings.TrimSpace(content)
	raw = strings.TrimLeftFunc(raw, func(r rune) bool { return r == '/' || unicode.IsSpace(r) })
	if raw == "" {
		return QQBotUserCommand{Kind: QQBotCmdHelp}
	}
	if bind, ok := parseQQBotBindCommand(raw); ok {
		return QQBotUserCommand{Kind: QQBotCmdBind, Bind: bind}
	}
	key := strings.ToLower(raw)
	for _, spec := range qqBotCommandCatalog() {
		for _, word := range spec.Keywords {
			if strings.ToLower(word) == key {
				return QQBotUserCommand{Kind: spec.Kind, Report: spec.Report}
			}
		}
	}
	return QQBotUserCommand{Kind: QQBotCmdUnknown}
}

func QQBotHelpText() string {
	var b strings.Builder
	b.WriteString("UpstreamOps 机器人口令（@我后发送）：\n")
	for _, spec := range qqBotCommandCatalog() {
		b.WriteString("\n")
		b.WriteString(spec.Keywords[0])
		b.WriteString(" — ")
		b.WriteString(spec.Summary)
	}
	return b.String()
}

func QQBotUnknownCommandReply() string {
	return "没看懂这条口令。发送「帮助」查看全部操作。"
}

func QQBotTestReply() string {
	return "这是一条来自 UpstreamOps 的测试消息。"
}

func TruncateQQBotReply(s string) string {
	s = strings.TrimSpace(s)
	if utf8.RuneCountInString(s) <= qqBotReplyMaxRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:qqBotReplyMaxRunes-1]) + "…"
}
