package rtk

import (
	"bytes"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// Middleware returns a Gin middleware that compresses oversized tool_result blocks
// in Anthropic and OpenAI format request bodies before they reach the handler.
func Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method != http.MethodPost {
			c.Next()
			return
		}

		body, err := io.ReadAll(c.Request.Body)
		if err != nil || len(body) < compressThreshold {
			if err == nil {
				c.Request.Body = io.NopCloser(bytes.NewReader(body))
			}
			c.Next()
			return
		}

		path := c.Request.URL.Path
		var compressed int
		var newBody []byte

		if IsAnthropicFormat(path, body) {
			newBody, compressed = CompressAnthropicBody(body)
		} else {
			newBody, compressed = CompressOpenAIBody(body)
		}

		if compressed > 0 {
			log.Infof("RTK: compressed %d tool_result(s), %d→%d bytes", compressed, len(body), len(newBody))
			body = newBody
		}

		c.Request.Body = io.NopCloser(bytes.NewReader(body))
		c.Request.ContentLength = int64(len(body))
		c.Next()
	}
}
