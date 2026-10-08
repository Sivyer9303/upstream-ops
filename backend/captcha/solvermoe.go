package captcha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/go-resty/resty/v2"
)

func init() {
	Register(storage.CaptchaSolverMoe, func(c Config) Provider { return newSolverMoe(c) })
}

// solverMoe 对接 https://solver.000.moe 的 TurnstileTaskProxyless。
//
// 异步接口与 AntiCaptcha 同源：
//
//	POST /createTask     -> { errorId, taskId }
//	POST /getTaskResult  -> { status: "ready", solution: { token } } 或 status: "processing"
//
// 文档要求每 3 秒轮询一次，通常 15–30 秒完成。这里最多等 120 秒。
// 求解浏览器不走代理；SetProxy 只影响访问求解 API 的 HTTP 客户端。
type solverMoe struct {
	cfg  Config
	http *resty.Client
	base string
}

func newSolverMoe(c Config) *solverMoe {
	base := c.Endpoint
	if base == "" {
		base = "https://solver.000.moe"
	}
	return &solverMoe{
		cfg:  c,
		http: resty.New().SetTimeout(30 * time.Second),
		base: base,
	}
}

func (p *solverMoe) SetProxy(proxyURL string) {
	if proxyURL != "" {
		p.http.SetProxy(proxyURL)
	}
}

type solverMoeCreateResp struct {
	ErrorID          int    `json:"errorId"`
	ErrorCode        string `json:"errorCode"`
	ErrorDescription string `json:"errorDescription"`
	TaskID           any    `json:"taskId"`
}

type solverMoeResultResp struct {
	ErrorID          int    `json:"errorId"`
	ErrorCode        string `json:"errorCode"`
	ErrorDescription string `json:"errorDescription"`
	Status           string `json:"status"` // "ready" | "processing"
	Solution         struct {
		Token string `json:"token"`
	} `json:"solution"`
}

func (p *solverMoe) SolveTurnstile(ctx context.Context, siteKey, pageURL string) (string, error) {
	if p.cfg.APIKey == "" {
		return "", errors.New("solvermoe: api key is empty")
	}
	if siteKey == "" {
		return "", errors.New("solvermoe: siteKey is empty")
	}

	taskID, err := p.createTask(ctx, siteKey, pageURL)
	if err != nil {
		return "", err
	}

	deadline := time.Now().Add(120 * time.Second)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return "", errors.New("solvermoe: timed out waiting for solution")
			}
			token, ready, err := p.fetchResult(ctx, taskID)
			if err != nil {
				return "", err
			}
			if ready {
				return token, nil
			}
		}
	}
}

func (p *solverMoe) createTask(ctx context.Context, siteKey, pageURL string) (string, error) {
	body := map[string]any{
		"clientKey": p.cfg.APIKey,
		"task": map[string]any{
			"type":       "TurnstileTaskProxyless",
			"websiteURL": pageURL,
			"websiteKey": siteKey,
		},
	}
	resp, err := p.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(body).
		Post(p.base + "/createTask")
	if err != nil {
		return "", fmt.Errorf("solvermoe createTask http: %w", err)
	}
	var r solverMoeCreateResp
	if err := json.Unmarshal(resp.Body(), &r); err != nil {
		return "", fmt.Errorf("solvermoe createTask decode: %w", err)
	}
	if r.ErrorID != 0 || r.TaskID == nil {
		return "", fmt.Errorf("solvermoe createTask: %s %s", r.ErrorCode, r.ErrorDescription)
	}
	switch v := r.TaskID.(type) {
	case string:
		if v == "" {
			return "", errors.New("solvermoe createTask: empty taskId")
		}
		return v, nil
	case float64:
		return fmt.Sprintf("%.0f", v), nil
	default:
		return fmt.Sprint(v), nil
	}
}

func (p *solverMoe) fetchResult(ctx context.Context, taskID string) (string, bool, error) {
	resp, err := p.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(map[string]any{
			"clientKey": p.cfg.APIKey,
			"taskId":    taskID,
		}).
		Post(p.base + "/getTaskResult")
	if err != nil {
		return "", false, fmt.Errorf("solvermoe getTaskResult http: %w", err)
	}
	var r solverMoeResultResp
	if err := json.Unmarshal(resp.Body(), &r); err != nil {
		return "", false, fmt.Errorf("solvermoe getTaskResult decode: %w", err)
	}
	if r.ErrorID != 0 {
		return "", false, fmt.Errorf("solvermoe getTaskResult: %s %s", r.ErrorCode, r.ErrorDescription)
	}
	if r.Status == "ready" {
		if r.Solution.Token == "" {
			return "", false, errors.New("solvermoe: ready but empty token")
		}
		return r.Solution.Token, true, nil
	}
	return "", false, nil
}
