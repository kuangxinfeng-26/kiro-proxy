// Package rtk implements Result Token Kompression — compresses large tool_result
// blocks in Anthropic and OpenAI format requests before forwarding upstream.
package rtk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const compressThreshold = 4000 // chars (~1000 tokens)

// ── Noise patterns ────────────────────────────────────────────────────────────

var (
	reDirs = regexp.MustCompile(
		`(?m)^.*[/\\](node_modules|\.git|\.next|dist|build|__pycache__|\.cache|\.venv|venv)[/\\]`,
	)
	reANSI      = regexp.MustCompile(`\x1b\[[0-9;]*[mK]`)
	reTimestamp = regexp.MustCompile(`\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}[.,]?\d*\s*`)
	reUUID      = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	reHex       = regexp.MustCompile(`0x[0-9a-fA-F]+`)
	reBigNum    = regexp.MustCompile(`\b\d{4,}\b`)
	rePath      = regexp.MustCompile(`/[\w./\-]+`)

	// Matches plain source code AND Claude Code Read output (line-number prefix "42\t")
	reCode = regexp.MustCompile(
		`(?m)^(\d+\t)?(import |from |def |class |async def |#!|// |/\*|package |using |#include|require\(|module\.exports|    def |    class |    async def )`,
	)

	reGrepLine     = regexp.MustCompile(`(?m)^([^\s:]+):(\d+):(.*)`)
	reJournalLine  = regexp.MustCompile(`\w{3}\s+\d+\s+\d+:\d+:\d+\s+\S+\s+\S+\[`)
	journalHint    = "You are currently not seeing messages from other users"
	reLogLevel     = regexp.MustCompile(`\b(ERROR|WARN|INFO|DEBUG|FATAL|CRITICAL)\b`)
	reGitDiffStart = regexp.MustCompile(`(?m)^--- .+\n\+\+\+ `)
)

// ── Tool detection ────────────────────────────────────────────────────────────

type contentKind string

const (
	kindCode    contentKind = "code"
	kindGrep    contentKind = "grep"
	kindGitDiff contentKind = "git_diff"
	kindLog     contentKind = "log"
	kindJSON    contentKind = "json"
	kindFind    contentKind = "find"
	kindLS      contentKind = "ls"
	kindGitLog  contentKind = "git_log"
	kindDefault contentKind = "default"
)

func detectKind(toolName, content string) contentKind {
	name := strings.ToLower(toolName)
	switch {
	case strings.Contains(name, "grep") || name == "rg":
		return kindGrep
	case strings.Contains(name, "find"):
		return kindFind
	case strings.Contains(name, "ls") || strings.Contains(name, "list_dir") || strings.Contains(name, "tree"):
		return kindLS
	case strings.Contains(name, "git") && strings.Contains(name, "diff"):
		return kindGitDiff
	case strings.Contains(name, "git") && strings.Contains(name, "log"):
		return kindGitLog
	case strings.Contains(name, "log"):
		return kindLog
	}

	// Content heuristics
	preview := content
	if len(preview) > 600 {
		preview = content[:600]
	}

	if strings.HasPrefix(content, "diff --git") || reGitDiffStart.MatchString(content[:min(200, len(content))]) {
		return kindGitDiff
	}
	if reGrepLine.MatchString(content[:min(500, len(content))]) {
		return kindGrep
	}
	if strings.Contains(content[:min(400, len(content))], journalHint) || reJournalLine.MatchString(content[:min(300, len(content))]) {
		return kindLog
	}
	stripped := reANSI.ReplaceAllString(content[:min(300, len(content))], "")
	if reLogLevel.MatchString(stripped) {
		return kindLog
	}
	if reCode.MatchString(preview) {
		return kindCode
	}
	var js json.RawMessage
	if json.Unmarshal([]byte(strings.TrimSpace(content)), &js) == nil {
		return kindJSON
	}
	return kindDefault
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ── Compressors ───────────────────────────────────────────────────────────────

func compressGrep(content string) string {
	type fileEntry struct {
		name  string
		lines []string
	}
	files := map[string]*fileEntry{}
	order := []string{}
	total := 0
	const maxPerFile = 30
	const maxTotal = 150

	for _, line := range strings.Split(content, "\n") {
		m := reGrepLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		fname, lineno, text := m[1], m[2], m[3]
		if _, ok := files[fname]; !ok {
			order = append(order, fname)
			files[fname] = &fileEntry{name: fname}
		}
		if len(files[fname].lines) < maxPerFile && total < maxTotal {
			if len(text) > 120 {
				text = text[:117] + "..."
			}
			files[fname].lines = append(files[fname].lines, fmt.Sprintf("  %s: %s", lineno, text))
			total++
		}
	}
	if len(files) == 0 {
		return content
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d matches in %d file(s):\n", total, len(files))
	for _, fname := range order {
		sb.WriteString(fname + ":\n")
		for _, l := range files[fname].lines {
			sb.WriteString(l + "\n")
		}
	}
	return strings.TrimRight(sb.String(), "\n")
}

func compressGitDiff(content string) string {
	var out []string
	var currentFile string
	var fileAdded, fileRemoved, added, removed int

	flushFile := func() {
		if currentFile != "" && (fileAdded > 0 || fileRemoved > 0) {
			out = append(out, fmt.Sprintf("[%s] +%d -%d", currentFile, fileAdded, fileRemoved))
		}
		fileAdded, fileRemoved = 0, 0
	}

	for _, line := range strings.Split(content, "\n") {
		if strings.HasPrefix(line, "diff --git ") {
			flushFile()
			m := regexp.MustCompile(` b/(.+)$`).FindStringSubmatch(line)
			if m != nil {
				currentFile = m[1]
			} else {
				currentFile = line
			}
		} else if strings.HasPrefix(line, "--- ") || strings.HasPrefix(line, "+++ ") ||
			strings.HasPrefix(line, "index ") || strings.HasPrefix(line, "new file") ||
			strings.HasPrefix(line, "deleted file") || strings.HasPrefix(line, "Binary") ||
			strings.HasPrefix(line, "@@") {
			continue
		} else if strings.HasPrefix(line, "+") {
			out = append(out, line)
			added++
			fileAdded++
		} else if strings.HasPrefix(line, "-") {
			out = append(out, line)
			removed++
			fileRemoved++
		}
	}
	flushFile()

	header := fmt.Sprintf("git diff: +%d -%d lines", added, removed)
	result := append([]string{header}, out...)
	const maxLines = 500
	if len(result) > maxLines {
		dropped := len(result) - maxLines
		result = append(result[:maxLines], fmt.Sprintf("... (%d more lines truncated)", dropped))
	}
	return strings.Join(result, "\n")
}

func compressLog(content string) string {
	errors := map[string]int{}
	warnings := map[string]int{}
	total := 0

	for _, raw := range strings.Split(content, "\n") {
		line := reANSI.ReplaceAllString(raw, "")
		norm := reTimestamp.ReplaceAllString(line, "")
		norm = reUUID.ReplaceAllString(norm, "<UUID>")
		norm = reHex.ReplaceAllString(norm, "<HEX>")
		norm = reBigNum.ReplaceAllString(norm, "<NUM>")
		norm = rePath.ReplaceAllString(norm, "<PATH>")
		norm = strings.TrimSpace(norm)
		if norm == "" {
			continue
		}
		total++
		low := strings.ToLower(norm)
		key := norm
		if len(key) > 120 {
			key = key[:120]
		}
		if containsAny(low, "error", "fatal", "panic", "exception", "traceback") {
			errors[key]++
		} else if containsAny(low, "warn", "warning") {
			warnings[key]++
		}
	}

	if len(errors) == 0 && len(warnings) == 0 {
		return content
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Log: %d lines total, %d unique errors, %d unique warnings\n", total, len(errors), len(warnings))

	if len(errors) > 0 {
		sb.WriteString("Top errors:\n")
		for _, kv := range topN(errors, 10) {
			fmt.Fprintf(&sb, "  [x%d] %s\n", kv[1], kv[0])
		}
	}
	if len(warnings) > 0 {
		sb.WriteString("Top warnings:\n")
		for _, kv := range topN(warnings, 5) {
			fmt.Fprintf(&sb, "  [x%d] %s\n", kv[1], kv[0])
		}
	}
	return strings.TrimRight(sb.String(), "\n")
}

func containsAny(s string, keywords ...string) bool {
	for _, k := range keywords {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}

func topN(m map[string]int, n int) [][2]any {
	type kv struct {
		k string
		v int
	}
	pairs := make([]kv, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, kv{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].v > pairs[j].v })
	if len(pairs) > n {
		pairs = pairs[:n]
	}
	result := make([][2]any, len(pairs))
	for i, p := range pairs {
		result[i] = [2]any{p.k, p.v}
	}
	return result
}

func compressJSON(content string) string {
	var data any
	if err := json.Unmarshal([]byte(strings.TrimSpace(content)), &data); err != nil {
		return content
	}
	walked := walkJSON(data, 0)
	out, err := json.MarshalIndent(walked, "", "  ")
	if err != nil {
		return content
	}
	return string(out)
}

func walkJSON(obj any, depth int) any {
	if depth > 6 {
		return "..."
	}
	switch v := obj.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		const maxKeys = 20
		result := map[string]any{}
		for i, k := range keys {
			if i >= maxKeys {
				result[fmt.Sprintf("... +%d more keys", len(keys)-maxKeys)] = nil
				break
			}
			result[k] = walkJSON(v[k], depth+1)
		}
		return result
	case []any:
		const maxArr = 5
		if len(v) <= maxArr {
			out := make([]any, len(v))
			for i, item := range v {
				out[i] = walkJSON(item, depth+1)
			}
			return out
		}
		return []any{walkJSON(v[0], depth+1), fmt.Sprintf("... +%d more items", len(v)-1)}
	case string:
		const maxStr = 80
		if len(v) > maxStr {
			return v[:maxStr-3] + "..."
		}
		return v
	default:
		return obj
	}
}

func compressDefault(content string, maxLines int) string {
	content = reDirs.ReplaceAllString(content, "")
	content = regexp.MustCompile(`\n{2,}`).ReplaceAllString(content, "\n")
	lines := strings.Split(content, "\n")
	if len(lines) <= maxLines {
		return content
	}
	dropped := len(lines) - maxLines
	return strings.Join(lines[:maxLines], "\n") + fmt.Sprintf("\n... (%d more lines truncated)", dropped)
}

var defaultMaxLines = map[contentKind]int{
	kindFind:   200,
	kindLS:     200,
	kindGitLog: 200,
}

// Compress compresses a single tool_result content string.
func Compress(toolName, content string) string {
	if len(content) < compressThreshold {
		return content
	}
	kind := detectKind(toolName, content)
	if kind == kindCode {
		return content
	}

	var result string
	switch kind {
	case kindGrep:
		result = compressGrep(content)
	case kindGitDiff:
		result = compressGitDiff(content)
	case kindLog:
		result = compressLog(content)
	case kindJSON:
		result = compressJSON(content)
	case kindFind, kindLS, kindGitLog:
		result = compressDefault(content, defaultMaxLines[kind])
	default:
		result = compressDefault(content, 300)
	}
	return result
}

// ── Anthropic format ──────────────────────────────────────────────────────────

// CompressAnthropicBody compresses tool_result blocks in an Anthropic-format request body.
// Returns the (possibly modified) body and the number of blocks compressed.
func CompressAnthropicBody(body []byte) ([]byte, int) {
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		return body, 0
	}

	messages, ok := req["messages"].([]any)
	if !ok {
		return body, 0
	}

	compressed := 0
	modified := false

	for _, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if !ok {
			continue
		}
		if msgMap["role"] != "user" {
			continue
		}
		content, ok := msgMap["content"].([]any)
		if !ok {
			continue
		}
		for i, block := range content {
			blockMap, ok := block.(map[string]any)
			if !ok {
				continue
			}
			if blockMap["type"] != "tool_result" {
				continue
			}
			text, ok := blockMap["content"].(string)
			if !ok || len(text) < compressThreshold {
				continue
			}
			toolName, _ := blockMap["tool_name"].(string)
			newText := Compress(toolName, text)
			if newText != text {
				blockMap["content"] = newText
				content[i] = blockMap
				compressed++
				modified = true
			}
		}
		if modified {
			msgMap["content"] = content
		}
	}

	if !modified {
		return body, 0
	}

	out, err := json.Marshal(req)
	if err != nil {
		return body, 0
	}
	return out, compressed
}

// ── OpenAI format ─────────────────────────────────────────────────────────────

// CompressOpenAIBody compresses tool message content in an OpenAI-format request body.
func CompressOpenAIBody(body []byte) ([]byte, int) {
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		return body, 0
	}

	messages, ok := req["messages"].([]any)
	if !ok {
		return body, 0
	}

	compressed := 0
	modified := false

	for _, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if !ok {
			continue
		}
		if msgMap["role"] != "tool" {
			continue
		}
		text, ok := msgMap["content"].(string)
		if !ok || len(text) < compressThreshold {
			continue
		}
		newText := Compress("", text)
		if newText != text {
			msgMap["content"] = newText
			compressed++
			modified = true
		}
	}

	if !modified {
		return body, 0
	}

	out, err := json.Marshal(req)
	if err != nil {
		return body, 0
	}
	return out, compressed
}

// IsAnthropicFormat returns true if the body looks like an Anthropic messages request.
func IsAnthropicFormat(path string, body []byte) bool {
	if strings.Contains(path, "/messages") {
		return true
	}
	return bytes.Contains(body[:min(100, len(body))], []byte(`"messages"`)) &&
		!bytes.Contains(body[:min(200, len(body))], []byte(`"choices"`))
}
