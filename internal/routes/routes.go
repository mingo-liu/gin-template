package routes

import (
	"gin-template/internal/handler"
	"gin-template/internal/middleware"
	"gin-template/web"
	"github.com/gin-gonic/gin"
	"net/http"
)

func New(h *handler.Handler) *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery(), middleware.SameOrigin())
	r.Use(func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("Cache-Control", "no-store")
		c.Next()
	})
	r.GET("/", func(c *gin.Context) {
		data, _ := web.Files.ReadFile("index.html")
		c.Data(200, "text/html; charset=utf-8", data)
	})
	api := r.Group("/api/v1")
	api.POST("/session", h.Session)
	api.GET("/targets", h.Articles)
	auth := api.Group("", h.Auth.Require(), middleware.RateLimit())
	auth.GET("/targets/:type/:id/like", h.State)
	auth.PUT("/targets/:type/:id/like", h.Change)
	auth.GET("/targets/:type/:id/sync", h.Sync)
	return r
}
