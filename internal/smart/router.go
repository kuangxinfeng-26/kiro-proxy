// Package smart implements automatic model routing for Kiro accounts.
// When the client requests "kiro-smart", the router selects haiku/sonnet/opus
// based on task type keywords and last-user-message token count.
package smart

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	log "github.com/sirupsen/logrus"
)

const SmartModel = "kiro-smart"

// target models (Kiro uses Anthropic model IDs directly)
const (
	modelHaiku  = "claude-haiku-4-5"
	modelSonnet = "claude-sonnet-4-6"
	modelOpus   = "claude-opus-4-6"
)

// token thresholds (estimated from last user message char count / 4)
const (
	thresholdOpus   = 6000 // tokens → opus
	thresholdSonnet = 800  // tokens → sonnet, else haiku
)

// keywords that suggest a simple/fast task → haiku
var haikusKeywords = []string{
	"翻译", "translate", "摘要", "summary", "summarize",
	"分类", "classify", "classification",
	"简单", "simple", "quick", "brief",
}

// keywords that suggest complex reasoning → opus
var opusKeywords = []string{
	"架构", "architecture", "设计", "design",
	"安全审计", "security audit", "复杂", "complex",
	"深度分析", "deep analysis", "全面", "comprehensive",
}

// Route selects the appropriate Kiro model for a "kiro-smart" request.
// It inspects the last user message content and token estimate.
func Route(body []byte) string {
	messages := gjson.GetBytes(body, "messages")
	if !messages.IsArray() {
		return modelSonnet
	}

	// Find last user message
	var lastUserContent string
	for _, msg := range messages.Array() {
		if msg.Get("role").String() == "user" {
			content := msg.Get("content")
			if content.Type == gjson.String {
				lastUserContent = content.String()
			} else if content.IsArray() {
				var parts []string
				for _, part := range content.Array() {
					if part.Get("type").String() == "text" {
						parts = append(parts, part.Get("text").String())
					}
				}
				lastUserContent = strings.Join(parts, "")
			}
		}
	}

	lower := strings.ToLower(lastUserContent)

	// Check haiku keywords first (fast tasks)
	for _, kw := range haikusKeywords {
		if strings.Contains(lower, kw) {
			log.Debugf("smart router: haiku (keyword=%q)", kw)
			return modelHaiku
		}
	}

	// Check opus keywords (complex tasks)
	for _, kw := range opusKeywords {
		if strings.Contains(lower, kw) {
			log.Debugf("smart router: opus (keyword=%q)", kw)
			return modelOpus
		}
	}

	// Token-based routing
	tokens := utf8.RuneCountInString(lastUserContent) / 4
	switch {
	case tokens >= thresholdOpus:
		log.Debugf("smart router: opus (tokens=%d)", tokens)
		return modelOpus
	case tokens >= thresholdSonnet:
		log.Debugf("smart router: sonnet (tokens=%d)", tokens)
		return modelSonnet
	default:
		log.Debugf("smart router: haiku (tokens=%d)", tokens)
		return modelHaiku
	}
}

// Middleware returns a Gin middleware that rewrites "kiro-smart" model requests
// to the appropriate actual model before the handler processes them.
func Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method != http.MethodPost {
			c.Next()
			return
		}

		body, err := io.ReadAll(c.Request.Body)
		if err != nil || len(body) == 0 {
			if err == nil {
				c.Request.Body = io.NopCloser(bytes.NewReader(body))
			}
			c.Next()
			return
		}

		// Quick check before full parse
		if !bytes.Contains(body, []byte(SmartModel)) {
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
			c.Next()
			return
		}

		model := gjson.GetBytes(body, "model").String()
		if !strings.EqualFold(strings.TrimSpace(model), SmartModel) {
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
			c.Next()
			return
		}

		target := Route(body)
		newBody, err := sjson.SetBytes(body, "model", target)
		if err != nil {
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
			c.Next()
			return
		}

		log.Infof("smart router: %s → %s", SmartModel, target)
		c.Request.Body = io.NopCloser(bytes.NewReader(newBody))
		c.Request.ContentLength = int64(len(newBody))
		c.Next()
	}
}

// suppress unused import
var _ = json.Marshal
