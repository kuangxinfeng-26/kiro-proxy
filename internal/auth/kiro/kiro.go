package kiro

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	refreshURLTemplate = "https://prod.%s.auth.desktop.kiro.dev/refreshToken"
	apiHostTemplate    = "https://q.%s.amazonaws.com"
	defaultRegion      = "us-east-1"
	userAgent          = "KiroIDE-0.7.45"
)

// Creds holds the runtime token state for a single Kiro account.
type Creds struct {
	mu           sync.Mutex
	RefreshToken string
	ProfileARN   string
	Region       string
	AccessToken  string
	ExpiresAt    time.Time
}

// NewCreds creates a Creds from the metadata map stored in an Auth file.
func NewCreds(metadata map[string]any) *Creds {
	c := &Creds{Region: defaultRegion}
	if v, ok := metadata["refresh_token"].(string); ok {
		c.RefreshToken = strings.TrimSpace(v)
	}
	if v, ok := metadata["profile_arn"].(string); ok {
		c.ProfileARN = strings.TrimSpace(v)
	}
	if v, ok := metadata["region"].(string); ok && strings.TrimSpace(v) != "" {
		c.Region = strings.TrimSpace(v)
	}
	if v, ok := metadata["access_token"].(string); ok {
		c.AccessToken = strings.TrimSpace(v)
	}
	if v, ok := metadata["expires_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			c.ExpiresAt = t
		}
	}
	return c
}

// APIHost returns the Q Developer API base URL for this account's region.
func (c *Creds) APIHost() string {
	region := c.Region
	if region == "" {
		region = defaultRegion
	}
	return fmt.Sprintf(apiHostTemplate, region)
}

// GetAccessToken returns a valid access token, refreshing if needed.
func (c *Creds) GetAccessToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Valid for more than 5 minutes — reuse
	if c.AccessToken != "" && time.Until(c.ExpiresAt) > 5*time.Minute {
		return c.AccessToken, nil
	}

	if c.RefreshToken == "" {
		return "", fmt.Errorf("kiro: no refresh_token available")
	}

	return c.refresh(ctx)
}

type refreshRequest struct {
	RefreshToken string `json:"refreshToken"`
}

type refreshResponse struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresIn    int    `json:"expiresIn"`
	ProfileARN   string `json:"profileArn"`
}

func (c *Creds) refresh(ctx context.Context) (string, error) {
	region := c.Region
	if region == "" {
		region = defaultRegion
	}
	url := fmt.Sprintf(refreshURLTemplate, region)

	body, err := json.Marshal(refreshRequest{RefreshToken: c.RefreshToken})
	if err != nil {
		return "", fmt.Errorf("kiro: marshal refresh request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("kiro: build refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("kiro: refresh request: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("kiro: read refresh response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("kiro: refresh failed status=%d body=%s", resp.StatusCode, string(data))
	}

	var result refreshResponse
	if err = json.Unmarshal(data, &result); err != nil {
		return "", fmt.Errorf("kiro: parse refresh response: %w", err)
	}
	if result.AccessToken == "" {
		return "", fmt.Errorf("kiro: refresh response missing accessToken")
	}

	c.AccessToken = result.AccessToken
	if result.RefreshToken != "" {
		c.RefreshToken = result.RefreshToken
	}
	if result.ProfileARN != "" {
		c.ProfileARN = result.ProfileARN
	}
	expiresIn := result.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}
	c.ExpiresAt = time.Now().Add(time.Duration(expiresIn-60) * time.Second)

	return c.AccessToken, nil
}
