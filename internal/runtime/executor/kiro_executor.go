package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	kiroauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kiro"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/google/uuid"
)

// KiroExecutor forwards requests to the AWS Q Developer (Kiro) API.
type KiroExecutor struct {
	cfg *config.Config
}

// NewKiroExecutor creates a new Kiro executor.
func NewKiroExecutor(cfg *config.Config) *KiroExecutor { return &KiroExecutor{cfg: cfg} }

// Identifier returns the executor identifier.
func (e *KiroExecutor) Identifier() string { return "kiro" }

// kiroCredsFromAuth extracts or builds a Creds object from an Auth entry.
func kiroCredsFromAuth(auth *cliproxyauth.Auth) *kiroauth.Creds {
	if auth == nil || auth.Metadata == nil {
		return nil
	}
	return kiroauth.NewCreds(auth.Metadata)
}

// Execute performs a non-streaming request to the Kiro API.
func (e *KiroExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	creds := kiroCredsFromAuth(auth)
	if creds == nil {
		return resp, fmt.Errorf("kiro executor: missing credentials")
	}

	token, err := creds.GetAccessToken(ctx)
	if err != nil {
		return resp, fmt.Errorf("kiro executor: get token: %w", err)
	}

	from := opts.SourceFormat
	to := sdktranslator.FromString("openai")
	baseModel := req.Model

	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	// Translate incoming request to OpenAI format first, then convert to Kiro format
	body := sdktranslator.TranslateRequest(from, to, baseModel, bytes.Clone(req.Payload), false)

	kiroPayload, err := buildKiroPayload(body, baseModel, creds.ProfileARN)
	if err != nil {
		return resp, fmt.Errorf("kiro executor: build payload: %w", err)
	}

	url := creds.APIHost() + "/generateAssistantResponse"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(kiroPayload))
	if err != nil {
		return resp, err
	}
	applyKiroHeaders(httpReq, token, false)

	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return resp, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		b, _ := io.ReadAll(httpResp.Body)
		err = statusErr{code: httpResp.StatusCode, msg: string(b)}
		return resp, err
	}

	// Kiro returns newline-delimited JSON events; collect all content
	content, inputTokens, outputTokens := collectKiroResponse(httpResp.Body)

	// Build OpenAI-compatible response
	openaiResp := buildOpenAIResponse(baseModel, content, inputTokens, outputTokens)
	reporter.Publish(ctx, helps.ParseOpenAIUsage(openaiResp))

	var param any
	out := sdktranslator.TranslateNonStream(ctx, to, from, req.Model, opts.OriginalRequest, body, openaiResp, &param)
	resp = cliproxyexecutor.Response{Payload: out, Headers: httpResp.Header.Clone()}
	return resp, nil
}

// ExecuteStream performs a streaming request to the Kiro API.
func (e *KiroExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	creds := kiroCredsFromAuth(auth)
	if creds == nil {
		return nil, fmt.Errorf("kiro executor: missing credentials")
	}

	token, err := creds.GetAccessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("kiro executor: get token: %w", err)
	}

	from := opts.SourceFormat
	to := sdktranslator.FromString("openai")
	baseModel := req.Model

	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	body := sdktranslator.TranslateRequest(from, to, baseModel, bytes.Clone(req.Payload), true)

	kiroPayload, err := buildKiroPayload(body, baseModel, creds.ProfileARN)
	if err != nil {
		return nil, fmt.Errorf("kiro executor: build payload: %w", err)
	}

	url := creds.APIHost() + "/generateAssistantResponse"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(kiroPayload))
	if err != nil {
		return nil, err
	}
	applyKiroHeaders(httpReq, token, true)

	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		b, _ := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		err = statusErr{code: httpResp.StatusCode, msg: string(b)}
		return nil, err
	}

	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		defer httpResp.Body.Close()

		var param any
		var inputTokens, outputTokens int

		// Read full response body then parse AWS EventStream frames
		raw, err := io.ReadAll(httpResp.Body)
		if err != nil {
			reporter.PublishFailure(ctx)
			select {
			case out <- cliproxyexecutor.StreamChunk{Err: err}:
			case <-ctx.Done():
			}
			return
		}

		pos := 0
		for pos < len(raw) {
			if pos+12 > len(raw) {
				break
			}
			totalLen := int(raw[pos])<<24 | int(raw[pos+1])<<16 | int(raw[pos+2])<<8 | int(raw[pos+3])
			headersLen := int(raw[pos+4])<<24 | int(raw[pos+5])<<16 | int(raw[pos+6])<<8 | int(raw[pos+7])
			if totalLen < 16 || pos+totalLen > len(raw) {
				break
			}
			payloadStart := pos + 12 + headersLen
			payloadEnd := pos + totalLen - 4
			if payloadEnd > payloadStart {
				framePayload := raw[payloadStart:payloadEnd]
				chunk, iTokens, oTokens := kiroEventToOpenAIChunk(framePayload, baseModel)
				inputTokens += iTokens
				outputTokens += oTokens

				if len(chunk) > 0 {
					sseChunk := append([]byte("data: "), chunk...)
					sseChunk = append(sseChunk, '\n', '\n')
					chunks := sdktranslator.TranslateStream(ctx, to, from, req.Model, opts.OriginalRequest, body, sseChunk, &param)
					for i := range chunks {
						select {
						case out <- cliproxyexecutor.StreamChunk{Payload: chunks[i]}:
						case <-ctx.Done():
							return
						}
					}
				}
			}
			pos += totalLen
		}

		// Emit usage chunk
		if inputTokens > 0 || outputTokens > 0 {
			usageChunk := buildOpenAIStreamUsageChunk(baseModel, inputTokens, outputTokens)
			sseChunk := append([]byte("data: "), usageChunk...)
			sseChunk = append(sseChunk, '\n', '\n')
			if detail, ok := helps.ParseOpenAIStreamUsage(usageChunk); ok {
				reporter.Publish(ctx, detail)
			}
			chunks := sdktranslator.TranslateStream(ctx, to, from, req.Model, opts.OriginalRequest, body, sseChunk, &param)
			for i := range chunks {
				select {
				case out <- cliproxyexecutor.StreamChunk{Payload: chunks[i]}:
				case <-ctx.Done():
					return
				}
			}
		}

		// Send [DONE]
		doneChunks := sdktranslator.TranslateStream(ctx, to, from, req.Model, opts.OriginalRequest, body, []byte("[DONE]"), &param)
		for i := range doneChunks {
			select {
			case out <- cliproxyexecutor.StreamChunk{Payload: doneChunks[i]}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return &cliproxyexecutor.StreamResult{Headers: httpResp.Header.Clone(), Chunks: out}, nil
}

// Refresh refreshes the Kiro access token.
func (e *KiroExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if auth == nil || auth.Metadata == nil {
		return auth, nil
	}
	creds := kiroCredsFromAuth(auth)
	if creds == nil || creds.RefreshToken == "" {
		return auth, nil
	}
	token, err := creds.GetAccessToken(ctx)
	if err != nil {
		return nil, err
	}
	auth.Metadata["access_token"] = token
	auth.Metadata["expires_at"] = creds.ExpiresAt.Format(time.RFC3339)
	if creds.RefreshToken != "" {
		auth.Metadata["refresh_token"] = creds.RefreshToken
	}
	return auth, nil
}

// applyKiroHeaders sets required headers for Kiro API requests.
func applyKiroHeaders(r *http.Request, token string, stream bool) {
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("User-Agent", "KiroIDE-0.7.45")
	if stream {
		r.Header.Set("Accept", "text/event-stream")
	} else {
		r.Header.Set("Accept", "application/json")
	}
}

// buildKiroPayload converts an OpenAI-format JSON body to Kiro's generateAssistantResponse format.
func buildKiroPayload(openaiBody []byte, model, profileARN string) ([]byte, error) {
	model = kiroModelID(model)
	conversationID := uuid.New().String()

	// Extract messages from OpenAI format
	messages := gjson.GetBytes(openaiBody, "messages")
	systemPrompt := ""
	var history []map[string]any
	var currentContent string

	if messages.IsArray() {
		msgs := messages.Array()
		// Extract system prompt
		for _, msg := range msgs {
			if msg.Get("role").String() == "system" {
				systemPrompt = msg.Get("content").String()
				break
			}
		}

		// Build history and current message
		userMsgs := make([]gjson.Result, 0)
		for _, msg := range msgs {
			if msg.Get("role").String() != "system" {
				userMsgs = append(userMsgs, msg)
			}
		}

		for i, msg := range userMsgs {
			role := msg.Get("role").String()
			content := extractMessageContent(msg)

			if i == len(userMsgs)-1 {
				// Last message becomes currentMessage
				currentContent = content
				if systemPrompt != "" && len(history) == 0 {
					currentContent = systemPrompt + "\n\n" + content
				}
			} else {
				// Add to history
				if role == "user" {
					entry := map[string]any{
						"userInputMessage": map[string]any{
							"content": content,
							"modelId": model,
							"origin":  "AI_EDITOR",
						},
					}
					if systemPrompt != "" && len(history) == 0 {
						entry["userInputMessage"].(map[string]any)["content"] = systemPrompt + "\n\n" + content
					}
					history = append(history, entry)
				} else if role == "assistant" {
					entry := map[string]any{
						"assistantResponseMessage": map[string]any{
							"content": content,
						},
					}
					history = append(history, entry)
				}
			}
		}
	}

	if currentContent == "" {
		currentContent = "Continue"
	}

	userInputMessage := map[string]any{
		"content": currentContent,
		"modelId": model,
		"origin":  "AI_EDITOR",
	}

	// Extract tools from OpenAI format
	tools := gjson.GetBytes(openaiBody, "tools")
	if tools.IsArray() && len(tools.Array()) > 0 {
		kiroTools := convertOpenAIToolsToKiro(tools.Array())
		if len(kiroTools) > 0 {
			userInputMessage["userInputMessageContext"] = map[string]any{
				"tools": kiroTools,
			}
		}
	}

	payload := map[string]any{
		"conversationState": map[string]any{
			"chatTriggerType": "MANUAL",
			"conversationId":  conversationID,
			"currentMessage": map[string]any{
				"userInputMessage": userInputMessage,
			},
		},
	}

	if len(history) > 0 {
		payload["conversationState"].(map[string]any)["history"] = history
	}

	if profileARN != "" {
		payload["profileArn"] = profileARN
	}

	return json.Marshal(payload)
}

// extractMessageContent extracts text content from an OpenAI message.
func extractMessageContent(msg gjson.Result) string {
	content := msg.Get("content")
	if content.Type == gjson.String {
		return content.String()
	}
	if content.IsArray() {
		var parts []string
		for _, part := range content.Array() {
			if part.Get("type").String() == "text" {
				parts = append(parts, part.Get("text").String())
			}
		}
		return strings.Join(parts, "")
	}
	return ""
}

// convertOpenAIToolsToKiro converts OpenAI tool definitions to Kiro format.
func convertOpenAIToolsToKiro(tools []gjson.Result) []map[string]any {
	result := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		if tool.Get("type").String() != "function" {
			continue
		}
		fn := tool.Get("function")
		name := fn.Get("name").String()
		if name == "" {
			continue
		}
		kiroTool := map[string]any{
			"toolSpecification": map[string]any{
				"name":        name,
				"description": fn.Get("description").String(),
				"inputSchema": map[string]any{
					"json": fn.Get("parameters").Raw,
				},
			},
		}
		result = append(result, kiroTool)
	}
	return result
}

// collectKiroResponse reads all Kiro AWS EventStream frames and returns the assembled content + token counts.
// Kiro uses AWS EventStream binary framing: each frame is [totalLen(4)][headersLen(4)][prelude_crc(4)][headers][payload][message_crc(4)].
// The payload JSON has the form {"content":"...","modelId":"..."} for text events.
func collectKiroResponse(body io.Reader) (content string, inputTokens, outputTokens int) {
	raw, err := io.ReadAll(body)
	if err != nil || len(raw) == 0 {
		return "", 0, 0
	}
	var sb strings.Builder
	pos := 0
	for pos < len(raw) {
		if pos+12 > len(raw) {
			break
		}
		totalLen := int(raw[pos])<<24 | int(raw[pos+1])<<16 | int(raw[pos+2])<<8 | int(raw[pos+3])
		headersLen := int(raw[pos+4])<<24 | int(raw[pos+5])<<16 | int(raw[pos+6])<<8 | int(raw[pos+7])
		if totalLen < 16 || pos+totalLen > len(raw) {
			break
		}
		payloadStart := pos + 12 + headersLen
		payloadEnd := pos + totalLen - 4
		if payloadEnd > payloadStart {
			var event map[string]any
			if err := json.Unmarshal(raw[payloadStart:payloadEnd], &event); err == nil {
				// Direct content field (assistantResponseEvent payload)
				if text, ok := event["content"].(string); ok && text != "" {
					sb.WriteString(text)
				}
				// Wrapped assistantResponseEvent (legacy)
				if assistantMsg, ok := event["assistantResponseEvent"].(map[string]any); ok {
					if text, ok := assistantMsg["content"].(string); ok {
						sb.WriteString(text)
					}
				}
				// Usage from messageMetadataEvent
				if usage, ok := event["messageMetadataEvent"].(map[string]any); ok {
					if u, ok := usage["usage"].(map[string]any); ok {
						if v, ok := u["inputTokens"].(float64); ok {
							inputTokens = int(v)
						}
						if v, ok := u["outputTokens"].(float64); ok {
							outputTokens = int(v)
						}
					}
				}
			}
		}
		pos += totalLen
	}
	return sb.String(), inputTokens, outputTokens
}

// kiroEventToOpenAIChunk converts a single Kiro NDJSON event to an OpenAI SSE chunk.
func kiroEventToOpenAIChunk(line []byte, model string) (chunk []byte, inputTokens, outputTokens int) {
	var event map[string]any
	if err := json.Unmarshal(line, &event); err != nil {
		return nil, 0, 0
	}

	// Text delta — direct content field (EventStream payload) or wrapped assistantResponseEvent
	if text, ok := event["content"].(string); ok && text != "" {
		sseData := map[string]any{
			"id":      "chatcmpl-kiro",
			"object":  "chat.completion.chunk",
			"model":   model,
			"choices": []map[string]any{{"index": 0, "delta": map[string]any{"role": "assistant", "content": text}, "finish_reason": nil}},
		}
		chunk, _ = json.Marshal(sseData)
		return chunk, 0, 0
	}
	if assistantMsg, ok := event["assistantResponseEvent"].(map[string]any); ok {
		if text, ok := assistantMsg["content"].(string); ok && text != "" {
			sseData := map[string]any{
				"id":      "chatcmpl-kiro",
				"object":  "chat.completion.chunk",
				"created": time.Now().Unix(),
				"model":   model,
				"choices": []map[string]any{
					{
						"index": 0,
						"delta": map[string]any{
							"content": text,
						},
						"finish_reason": nil,
					},
				},
			}
			chunk, _ = json.Marshal(sseData)
			return chunk, 0, 0
		}
	}

	// Tool use
	if toolEvent, ok := event["toolUseEvent"].(map[string]any); ok {
		toolID, _ := toolEvent["toolUseId"].(string)
		toolName, _ := toolEvent["name"].(string)
		toolInput, _ := toolEvent["input"].(string)

		if toolName != "" {
			sseData := map[string]any{
				"id":      "chatcmpl-kiro",
				"object":  "chat.completion.chunk",
				"created": time.Now().Unix(),
				"model":   model,
				"choices": []map[string]any{
					{
						"index": 0,
						"delta": map[string]any{
							"tool_calls": []map[string]any{
								{
									"index": 0,
									"id":    toolID,
									"type":  "function",
									"function": map[string]any{
										"name":      toolName,
										"arguments": toolInput,
									},
								},
							},
						},
						"finish_reason": nil,
					},
				},
			}
			chunk, _ = json.Marshal(sseData)
			return chunk, 0, 0
		}
	}

	// Usage
	if usage, ok := event["messageMetadataEvent"].(map[string]any); ok {
		if u, ok := usage["usage"].(map[string]any); ok {
			if v, ok := u["inputTokens"].(float64); ok {
				inputTokens = int(v)
			}
			if v, ok := u["outputTokens"].(float64); ok {
				outputTokens = int(v)
			}
		}
	}

	// Stop event
	if _, ok := event["messageStopEvent"]; ok {
		sseData := map[string]any{
			"id":      "chatcmpl-kiro",
			"object":  "chat.completion.chunk",
			"created": time.Now().Unix(),
			"model":   model,
			"choices": []map[string]any{
				{
					"index":         0,
					"delta":         map[string]any{},
					"finish_reason": "stop",
				},
			},
		}
		chunk, _ = json.Marshal(sseData)
	}

	return chunk, inputTokens, outputTokens
}

// buildOpenAIResponse builds a complete OpenAI non-streaming response from Kiro content.
func buildOpenAIResponse(model, content string, inputTokens, outputTokens int) []byte {
	resp := map[string]any{
		"id":      "chatcmpl-kiro",
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{
			{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": content,
				},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]any{
			"prompt_tokens":     inputTokens,
			"completion_tokens": outputTokens,
			"total_tokens":      inputTokens + outputTokens,
		},
	}
	data, _ := json.Marshal(resp)
	return data
}

// buildOpenAIStreamUsageChunk builds a usage SSE chunk for streaming.
func buildOpenAIStreamUsageChunk(model string, inputTokens, outputTokens int) []byte {
	chunk := map[string]any{
		"id":      "chatcmpl-kiro",
		"object":  "chat.completion.chunk",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{},
		"usage": map[string]any{
			"prompt_tokens":     inputTokens,
			"completion_tokens": outputTokens,
			"total_tokens":      inputTokens + outputTokens,
		},
	}
	data, _ := json.Marshal(chunk)
	return data
}

// CountTokens estimates token count for Kiro requests (delegates to Execute).
func (e *KiroExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, fmt.Errorf("kiro executor: CountTokens not supported")
}

// HttpRequest injects Kiro credentials into the request and executes it.
func (e *KiroExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("kiro executor: request is nil")
	}
	creds := kiroCredsFromAuth(auth)
	if creds != nil {
		token, err := creds.GetAccessToken(ctx)
		if err != nil {
			return nil, err
		}
		applyKiroHeaders(req, token, false)
	}
	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	return httpClient.Do(req.WithContext(ctx))
}

// kiroModelID maps a model alias to the Kiro internal model ID.
func kiroModelID(model string) string {
	if strings.HasPrefix(strings.ToLower(model), "kiro-") {
		model = model[5:]
	}
	// Map OpenAI-style IDs (dashes) to Kiro internal IDs (dots for version numbers).
	// e.g. claude-haiku-4-5 -> claude-haiku-4.5
	kiroNames := map[string]string{
		"claude-haiku-4-5":   "claude-haiku-4.5",
		"claude-sonnet-4-5":  "claude-sonnet-4.5",
		"claude-sonnet-4-6":  "claude-sonnet-4.6",
		"claude-opus-4-5":    "claude-opus-4.5",
		"claude-opus-4-6":    "claude-opus-4.6",
		"claude-opus-4-7":    "claude-opus-4.7",
		"claude-sonnet-4":    "claude-sonnet-4",
		"claude-3-7-sonnet":  "claude-3.7-sonnet",
	}
	if mapped, ok := kiroNames[strings.ToLower(model)]; ok {
		return mapped
	}
	return model
}

// suppress unused import warning
var _ = sjson.SetBytes
var _ = log.Debugf
