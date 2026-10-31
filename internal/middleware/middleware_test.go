package middleware

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticationAndOrigin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	auth := Auth{Secret: "a-test-secret-with-at-least-32-bytes"}
	r := gin.New()
	r.Use(SameOrigin(), auth.Require())
	r.PUT("/", func(c *gin.Context) { c.JSON(200, gin.H{"user_id": c.GetString("user_id")}) })
	for _, tc := range []struct {
		token, origin string
		status        int
	}{{"", "", 401}, {auth.Token("1001") + "bad", "", 401}, {auth.Token("1001"), "https://evil.example", 403}, {auth.Token("1001"), "http://example.com", 200}} {
		req := httptest.NewRequest(http.MethodPut, "http://example.com/", nil)
		req.Header.Set("Authorization", "Bearer "+tc.token)
		req.Header.Set("Origin", tc.origin)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Errorf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
		}
	}
}
