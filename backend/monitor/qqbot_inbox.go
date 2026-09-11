package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/bejix/upstream-ops/backend/channel"
	"github.com/bejix/upstream-ops/backend/notify"
	"github.com/bejix/upstream-ops/backend/storage"
)

// QQBotInbox 把群口令转成当前监控状态回复，只回给提问的那个群。
type QQBotInbox struct {
	channels      *storage.Channels
	announcements *storage.UpstreamAnnouncements
	rates         *storage.Rates
	monitorLogs   *storage.MonitorLogs
	notifies      *storage.Notifications
	captchas      *storage.Captchas
	channelSvc    *channel.Service
	log           *slog.Logger
}

func NewQQBotInbox(
	channels *storage.Channels,
	announcements *storage.UpstreamAnnouncements,
	rates *storage.Rates,
	monitorLogs *storage.MonitorLogs,
	notifies *storage.Notifications,
	captchas *storage.Captchas,
	channelSvc *channel.Service,
	log *slog.Logger,
) *QQBotInbox {
	if log == nil {
		log = slog.Default()
	}
	return &QQBotInbox{
		channels:      channels,
		announcements: announcements,
		rates:         rates,
		monitorLogs:   monitorLogs,
		notifies:      notifies,
		captchas:      captchas,
		channelSvc:    channelSvc,
		log:           log,
	}
}

func (s *QQBotInbox) HandleQQBotRateQuery(_ context.Context, arg string) string {
	if s == nil || s.channels == nil {
		return "还没有渠道数据。"
	}
	list, err := s.channels.ListMonitorEnabled()
	if err != nil {
		s.log.Error("qqbot rate query list channels", "err", err)
		return "查询失败：" + err.Error()
	}
	selector, filter := splitQQBotRateArg(arg)
	if selector == "" {
		counts := make(map[uint]int, len(list))
		if s.rates != nil {
			for _, ch := range list {
				rows, rateErr := s.rates.ListByChannel(ch.ID)
				if rateErr != nil {
					s.log.Error("qqbot rate query count", "channel", ch.Name, "err", rateErr)
					continue
				}
				counts[ch.ID] = len(rows)
			}
		}
		return formatQQBotRateCatalog(list, counts)
	}
	ch, errMsg := matchQQBotRateChannel(list, selector)
	if errMsg != "" {
		return errMsg
	}
	if s.rates == nil {
		return ch.Name + " 还没有倍率快照。"
	}
	rows, err := s.rates.ListByChannel(ch.ID)
	if err != nil {
		s.log.Error("qqbot rate query snapshots", "channel", ch.Name, "err", err)
		return "查询失败：" + err.Error()
	}
	return formatQQBotRateDetail(ch, rows, filter)
}

func splitQQBotRateArg(arg string) (selector, filter string) {
	fields := strings.Fields(strings.TrimSpace(arg))
	if len(fields) == 0 {
		return "", ""
	}
	if len(fields) == 1 {
		return fields[0], ""
	}
	return fields[0], strings.Join(fields[1:], " ")
}

func matchQQBotRateChannel(list []storage.Channel, selector string) (storage.Channel, string) {
	selector = strings.TrimSpace(selector)
	if len(list) == 0 {
		return storage.Channel{}, "还没有启用监控的上游渠道。"
	}
	if selector == "" {
		return storage.Channel{}, "发送 #倍率 查看上游列表。"
	}
	if id, err := strconv.ParseUint(selector, 10, 64); err == nil && id > 0 {
		for _, ch := range list {
			if uint64(ch.ID) == id {
				return ch, ""
			}
		}
		return storage.Channel{}, "没有这个上游编号。发送 #倍率 查看列表和查询命令。"
	}
	lower := strings.ToLower(selector)
	var exact []storage.Channel
	var contains []storage.Channel
	for _, ch := range list {
		name := strings.ToLower(strings.TrimSpace(ch.Name))
		if name == lower {
			exact = append(exact, ch)
			continue
		}
		if strings.Contains(name, lower) {
			contains = append(contains, ch)
		}
	}
	switch {
	case len(exact) == 1:
		return exact[0], ""
	case len(exact) > 1:
		return storage.Channel{}, formatQQBotRateAmbiguous(exact)
	case len(contains) == 1:
		return contains[0], ""
	case len(contains) > 1:
		return storage.Channel{}, formatQQBotRateAmbiguous(contains)
	default:
		return storage.Channel{}, "没有找到这个上游。发送 #倍率 查看列表和查询命令。"
	}
}

func formatQQBotRateAmbiguous(list []storage.Channel) string {
	var b strings.Builder
	b.WriteString("匹配到多个上游，请用更精确的名称或编号：\n")
	for _, ch := range list {
		fmt.Fprintf(&b, "· %s\n  #倍率#%d\n", ch.Name, ch.ID)
	}
	return strings.TrimSpace(b.String())
}

func formatQQBotRateCatalog(list []storage.Channel, counts map[uint]int) string {
	if len(list) == 0 {
		return "还没有启用监控的上游渠道。"
	}
	var b strings.Builder
	b.WriteString("当前上游渠道，再发对应命令查看倍率：\n")
	for _, ch := range list {
		n := 0
		if counts != nil {
			n = counts[ch.ID]
		}
		fmt.Fprintf(&b, "\n· %s  %d 条\n  #倍率#%d\n", ch.Name, n, ch.ID)
	}
	b.WriteString("\n也可以发 #倍率 渠道名，或 #倍率#ID 关键词 筛选。")
	return strings.TrimSpace(b.String())
}

func formatQQBotRateDetail(ch storage.Channel, rows []storage.RateSnapshot, filter string) string {
	filter = strings.TrimSpace(filter)
	filtered := rows
	if filter != "" {
		filtered = filtered[:0:0]
		lower := strings.ToLower(filter)
		for _, row := range rows {
			name := strings.ToLower(row.ModelName)
			desc := strings.ToLower(row.Description)
			if strings.Contains(name, lower) || strings.Contains(desc, lower) {
				filtered = append(filtered, row)
			}
		}
	}
	if len(filtered) == 0 {
		if filter != "" {
			return fmt.Sprintf("%s 没有匹配「%s」的倍率。", ch.Name, filter)
		}
		return ch.Name + " 还没有倍率快照。"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s 当前倍率（%d 条）\n", ch.Name, len(filtered))
	hidden := 0
	for i, row := range filtered {
		line := formatQQBotRateLine(row)
		next := b.String() + line
		if utf8.RuneCountInString(next) > 1700 {
			hidden = len(filtered) - i
			break
		}
		b.WriteString(line)
	}
	if hidden > 0 {
		fmt.Fprintf(&b, "…还有 %d 条，可用 #倍率#%d 关键词 筛选", hidden, ch.ID)
	}
	return strings.TrimSpace(b.String())
}

func formatQQBotRateLine(row storage.RateSnapshot) string {
	if row.CompletionRatio != 0 {
		return fmt.Sprintf("%s  %.4f / 补全 %.4f\n", row.ModelName, row.Ratio, row.CompletionRatio)
	}
	return fmt.Sprintf("%s  %.4f\n", row.ModelName, row.Ratio)
}

func (s *QQBotInbox) HandleQQBotReport(ctx context.Context, report string) string {
	if s == nil {
		return "查询服务未就绪。"
	}
	var (
		text string
		err  error
	)
	switch report {
	case notify.QQBotReportBalanceLow:
		text, err = s.reportBalanceLow()
	case notify.QQBotReportBalanceDigest:
		text, err = s.reportBalanceDigest()
	case notify.QQBotReportRateChanged:
		text, err = s.reportRateChanges(false)
	case notify.QQBotReportRateGroup:
		text, err = s.reportRateGroupChanges()
	case notify.QQBotReportAnnouncement:
		text, err = s.reportAnnouncements()
	case notify.QQBotReportLoginFailed:
		text, err = s.reportFailedLogs(storage.MonitorJobLogin, storage.EventLoginFailed, "登录失败")
	case notify.QQBotReportCaptchaFailed:
		text, err = s.reportCaptchaFailed()
	case notify.QQBotReportMonitorFailed:
		text, err = s.reportFailedLogs(storage.MonitorJobBalance, storage.EventMonitorFailed, "采集失败")
	case notify.QQBotReportSubscription:
		text, err = s.reportSubscriptions(ctx)
	default:
		return notify.QQBotUnknownCommandReply()
	}
	if err != nil {
		s.log.Error("qqbot report failed", "report", report, "err", err)
		return "查询失败：" + err.Error()
	}
	return text
}

func (s *QQBotInbox) channelNames() (map[uint]string, error) {
	if s.channels == nil {
		return map[uint]string{}, nil
	}
	list, err := s.channels.List()
	if err != nil {
		return nil, err
	}
	names := make(map[uint]string, len(list))
	for i := range list {
		names[list[i].ID] = list[i].Name
	}
	return names, nil
}

func (s *QQBotInbox) reportBalanceDigest() (string, error) {
	if s.channels == nil {
		return "还没有渠道数据。", nil
	}
	list, err := s.channels.ListMonitorEnabled()
	if err != nil {
		return "", err
	}
	_, body := formatBalanceDigest(list)
	return body, nil
}

func (s *QQBotInbox) reportBalanceLow() (string, error) {
	if s.channels == nil {
		return "还没有渠道数据。", nil
	}
	list, err := s.channels.ListMonitorEnabled()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	count := 0
	for _, c := range list {
		if c.BalanceThreshold <= 0 || c.LastBalance == nil || *c.LastBalance >= c.BalanceThreshold {
			continue
		}
		count++
		fmt.Fprintf(&b, "%d. %s  余额 %.4f  阈值 %.4f\n", count, c.Name, *c.LastBalance, c.BalanceThreshold)
	}
	if count == 0 {
		return "当前没有低于阈值的渠道。", nil
	}
	return fmt.Sprintf("余额不足（%d 个渠道）\n%s", count, strings.TrimSpace(b.String())), nil
}

func (s *QQBotInbox) reportRateChanges(groupOnly bool) (string, error) {
	if s.rates == nil {
		return "还没有倍率记录。", nil
	}
	logs, err := s.rates.ListChanges(0, 8)
	if err != nil {
		return "", err
	}
	names, err := s.channelNames()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	n := 0
	for _, item := range logs {
		added := item.OldRatio == nil
		if groupOnly && !added {
			continue
		}
		if !groupOnly && added {
			continue
		}
		n++
		chName := names[item.ChannelID]
		if chName == "" {
			chName = fmt.Sprintf("渠道#%d", item.ChannelID)
		}
		if added {
			fmt.Fprintf(&b, "%d. %s / %s  新增 %.4f  %s\n", n, chName, item.ModelName, item.NewRatio, item.ChangedAt.Local().Format("01-02 15:04"))
			continue
		}
		fmt.Fprintf(&b, "%d. %s / %s  %.4f → %.4f  %s\n", n, chName, item.ModelName, *item.OldRatio, item.NewRatio, item.ChangedAt.Local().Format("01-02 15:04"))
	}
	if n == 0 {
		if groupOnly {
			return "最近没有分组增减记录。", nil
		}
		return "最近没有倍率变化。", nil
	}
	title := "最近倍率变化"
	if groupOnly {
		title = "最近分组变动"
	}
	return fmt.Sprintf("%s\n%s", title, strings.TrimSpace(b.String())), nil
}

func (s *QQBotInbox) reportRateGroupChanges() (string, error) {
	text, err := s.reportRecentNotifyEvents([]storage.NotificationEvent{
		storage.EventRateStructureChanged,
		storage.EventRateAdded,
		storage.EventRateRemoved,
	}, 6, "最近分组变动")
	if err != nil {
		return "", err
	}
	if !strings.Contains(text, "暂无记录") {
		return text, nil
	}
	return s.reportRateChanges(true)
}

func (s *QQBotInbox) reportAnnouncements() (string, error) {
	if s.announcements == nil {
		return "还没有公告数据。", nil
	}
	list, err := s.announcements.ListLatest(5)
	if err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "最近没有上游公告。", nil
	}
	names, err := s.channelNames()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("最近上游公告\n")
	for i, item := range list {
		chName := names[item.ChannelID]
		if chName == "" {
			chName = fmt.Sprintf("渠道#%d", item.ChannelID)
		}
		title := strings.TrimSpace(item.Title)
		if title == "" {
			title = strings.TrimSpace(item.Content)
		}
		title = clipLine(title, 80)
		when := item.FirstSeenAt.Local().Format("01-02 15:04")
		if item.PublishedAt != nil && !item.PublishedAt.IsZero() {
			when = item.PublishedAt.Local().Format("01-02 15:04")
		}
		fmt.Fprintf(&b, "%d. [%s] %s  %s\n", i+1, chName, title, when)
	}
	return strings.TrimSpace(b.String()), nil
}

func (s *QQBotInbox) reportFailedLogs(job storage.MonitorJob, event storage.NotificationEvent, title string) (string, error) {
	var b strings.Builder
	n := 0
	if s.monitorLogs != nil {
		logs, err := s.monitorLogs.List(0, 30)
		if err != nil {
			return "", err
		}
		names, err := s.channelNames()
		if err != nil {
			return "", err
		}
		for _, item := range logs {
			if item.Success {
				continue
			}
			switch job {
			case storage.MonitorJobLogin:
				if item.Job != storage.MonitorJobLogin {
					continue
				}
			case storage.MonitorJobBalance:
				if item.Job != storage.MonitorJobBalance && item.Job != storage.MonitorJobRates {
					continue
				}
			}
			n++
			chName := names[item.ChannelID]
			if chName == "" {
				chName = fmt.Sprintf("渠道#%d", item.ChannelID)
			}
			fmt.Fprintf(&b, "%d. %s  %s  %s\n", n, chName, clipLine(item.ErrorMessage, 80), item.StartedAt.Local().Format("01-02 15:04"))
			if n >= 6 {
				break
			}
		}
	}
	if n == 0 {
		text, err := s.reportRecentNotifyEvents([]storage.NotificationEvent{event}, 6, title)
		if err == nil && text != "" {
			return text, nil
		}
		return "最近没有" + title + "记录。", nil
	}
	return fmt.Sprintf("最近%s\n%s", title, strings.TrimSpace(b.String())), nil
}

func (s *QQBotInbox) reportCaptchaFailed() (string, error) {
	var b strings.Builder
	n := 0
	if s.captchas != nil {
		list, err := s.captchas.List()
		if err != nil {
			return "", err
		}
		for _, item := range list {
			if strings.TrimSpace(item.BalanceError) == "" {
				continue
			}
			n++
			fmt.Fprintf(&b, "%d. %s  %s\n", n, item.Name, clipLine(item.BalanceError, 80))
		}
	}
	if n == 0 {
		return s.reportRecentNotifyEvents([]storage.NotificationEvent{storage.EventCaptchaFailed}, 6, "验证码失败")
	}
	return "打码平台异常\n" + strings.TrimSpace(b.String()), nil
}

func (s *QQBotInbox) reportSubscriptions(ctx context.Context) (string, error) {
	if s.channels == nil || s.channelSvc == nil {
		return s.reportRecentNotifyEvents([]storage.NotificationEvent{
			storage.EventSubscriptionDailyLow,
			storage.EventSubscriptionWeeklyLow,
			storage.EventSubscriptionMonthlyLow,
			storage.EventSubscriptionExpiring,
		}, 6, "订阅通知")
	}
	list, err := s.channels.ListMonitorEnabled()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	n := 0
	for i := range list {
		c := list[i]
		if !c.SubscriptionEnabled || c.Type != storage.ChannelTypeSub2API {
			continue
		}
		info, err := s.channelSvc.GetSubscriptionUsage(ctx, c.ID)
		if err != nil {
			n++
			fmt.Fprintf(&b, "%d. %s  查询失败：%s\n", n, c.Name, clipLine(err.Error(), 60))
			continue
		}
		if info == nil || len(info.Items) == 0 {
			continue
		}
		for _, item := range info.Items {
			n++
			line := fmt.Sprintf("%d. %s / %s", n, c.Name, subscriptionGroupName(item))
			if item.Monthly != nil && item.Monthly.LimitUSD > 0 {
				line += fmt.Sprintf("  月剩余 %.1f%%", item.Monthly.RemainingPercent)
			}
			if item.Daily != nil && item.Daily.LimitUSD > 0 {
				line += fmt.Sprintf("  日剩余 %.1f%%", item.Daily.RemainingPercent)
			}
			if item.ExpiresAt != nil && !item.ExpiresAt.IsZero() {
				line += "  到期 " + item.ExpiresAt.Local().Format("01-02")
			}
			b.WriteString(line)
			b.WriteByte('\n')
			if n >= 8 {
				break
			}
		}
		if n >= 8 {
			break
		}
	}
	if n == 0 {
		return s.reportRecentNotifyEvents([]storage.NotificationEvent{
			storage.EventSubscriptionDailyLow,
			storage.EventSubscriptionWeeklyLow,
			storage.EventSubscriptionMonthlyLow,
			storage.EventSubscriptionExpiring,
		}, 6, "订阅通知")
	}
	return "订阅状态\n" + strings.TrimSpace(b.String()), nil
}

func (s *QQBotInbox) reportRecentNotifyEvents(events []storage.NotificationEvent, limit int, title string) (string, error) {
	if s.notifies == nil {
		return title + "：暂无记录。", nil
	}
	if limit <= 0 {
		limit = 6
	}
	logs, err := s.notifies.ListLogs(40)
	if err != nil {
		return "", err
	}
	allow := make(map[storage.NotificationEvent]bool, len(events))
	for _, ev := range events {
		allow[ev] = true
	}
	var b strings.Builder
	n := 0
	for _, item := range logs {
		if !allow[item.Event] {
			continue
		}
		n++
		body := strings.TrimSpace(item.Subject)
		if item.Body != "" {
			if body != "" {
				body += " / "
			}
			body += clipLine(item.Body, 70)
		}
		fmt.Fprintf(&b, "%d. %s  %s\n", n, body, item.SentAt.Local().Format("01-02 15:04"))
		if n >= limit {
			break
		}
	}
	if n == 0 {
		return title + "：暂无记录。", nil
	}
	return title + "\n" + strings.TrimSpace(b.String()), nil
}

func formatBalanceDigest(list []storage.Channel) (string, string) {
	if len(list) == 0 {
		return "余额汇总", "余额汇总：当前没有启用监控的渠道。"
	}
	var total float64
	var b strings.Builder
	fmt.Fprintf(&b, "余额汇总（共 %d 个渠道）\n", len(list))
	for i, c := range list {
		bal := 0.0
		if c.LastBalance != nil {
			bal = *c.LastBalance
			total += bal
		}
		status := "健康"
		switch {
		case c.LastError != "":
			status = "异常"
		case c.BalanceThreshold > 0 && c.LastBalance != nil && *c.LastBalance < c.BalanceThreshold:
			status = "偏低"
		case c.LastBalance == nil:
			status = "未采样"
		}
		fmt.Fprintf(&b, "%d. %s  余额 %.4f", i+1, c.Name, bal)
		if c.BalanceThreshold > 0 {
			fmt.Fprintf(&b, "  阈值 %.4f", c.BalanceThreshold)
		}
		fmt.Fprintf(&b, "  %s\n", status)
	}
	fmt.Fprintf(&b, "\n合计：%.4f", total)
	subject := fmt.Sprintf("余额汇总（%d 渠道，合计 %.4f）", len(list), total)
	return subject, strings.TrimSpace(b.String())
}

func clipLine(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ")
	runes := []rune(s)
	if max <= 0 || len(runes) <= max {
		return s
	}
	return string(runes[:max-1]) + "…"
}
