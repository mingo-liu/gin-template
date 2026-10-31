package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"likeflow/internal/model"
)

type Auth struct{ Secret string }

// Token signs an expiring identity; production callers obtain it from their login service.
func (a Auth) Token(user string) string {
	payload := user + "." + strconv.FormatInt(time.Now().Add(12*time.Hour).Unix(), 10)
	mac := hmac.New(sha256.New, []byte(a.Secret))
	mac.Write([]byte(payload))
	return payload + "." + hex.EncodeToString(mac.Sum(nil))
}

func (a Auth) Require() gin.HandlerFunc {
	return func(c *gin.Context) {
		token, _ := c.Cookie("likeflow_session")
		if header := c.GetHeader("Authorization"); strings.HasPrefix(header, "Bearer ") {
			token = strings.TrimPrefix(header, "Bearer ")
		}
		parts := strings.Split(token, ".")
		if len(parts) != 3 {
			unauthorized(c)
			return
		}
		if _, err := model.Decimal(parts[0], ^uint64(0), true); err != nil {
			unauthorized(c)
			return
		}
		expires, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || expires <= time.Now().Unix() {
			unauthorized(c)
			return
		}
		sig, err := hex.DecodeString(parts[2])
		if err != nil {
			unauthorized(c)
			return
		}
		mac := hmac.New(sha256.New, []byte(a.Secret))
		mac.Write([]byte(parts[0] + "." + parts[1]))
		if !hmac.Equal(sig, mac.Sum(nil)) {
			unauthorized(c)
			return
		}
		c.Set("user_id", parts[0])
		c.Next()
	}
}

func unauthorized(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "请先选择测试用户登录", "code": "UNAUTHORIZED"})
}

// SameOrigin prevents cookie-authenticated cross-site writes, including demo login.
func SameOrigin() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method != "GET" && c.Request.Method != "HEAD" {
			if origin := c.GetHeader("Origin"); origin != "" {
				u, err := url.Parse(origin)
				scheme := "http"
				if c.Request.TLS != nil {
					scheme = "https"
				}
				if err != nil || u.Host != c.Request.Host || u.Scheme != scheme {
					c.AbortWithStatusJSON(403, gin.H{"error": "跨站请求被拒绝"})
					return
				}
			}
		}
		c.Next()
	}
}

type bucket struct {
	second int64
	count  int
}

// RateLimit bounds local user and target traffic without an unbounded identity map.
func RateLimit() gin.HandlerFunc {
	var mu sync.Mutex
	buckets := map[string]bucket{}
	return func(c *gin.Context) {
		now := time.Now().Unix()
		user := c.GetString("user_id")
		target := c.Param("type") + ":" + c.Param("id")
		mu.Lock()
		if len(buckets) > 10000 {
			for k, b := range buckets {
				if b.second < now {
					delete(buckets, k)
				}
			}
		}
		allowed := len(buckets) < 10001
		for _, item := range []struct {
			key   string
			limit int
		}{{"u:" + user, 30}, {"t:" + target, 200}} {
			b, exists := buckets[item.key]
			if !exists && len(buckets) >= 10000 {
				allowed = false
				continue
			}
			if b.second != now {
				b = bucket{second: now}
			}
			b.count++
			buckets[item.key] = b
			if b.count > item.limit {
				allowed = false
			}
		}
		mu.Unlock()
		if !allowed {
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(429, gin.H{"error": fmt.Sprint("操作过于频繁，请稍后重试"), "code": "RATE_LIMITED"})
			return
		}
		c.Next()
	}
}
