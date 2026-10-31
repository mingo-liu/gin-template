package handler

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"likeflow/internal/middleware"
	"likeflow/internal/model"
	"likeflow/internal/service"
)

type Handler struct {
	Service *service.Service
	Auth    middleware.Auth
	Demo    bool
}

func (h *Handler) Session(c *gin.Context) {
	if !h.Demo {
		c.JSON(404, gin.H{"error": "测试登录未启用"})
		return
	}
	var input struct {
		UserID string `json:"user_id"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || (input.UserID != "1001" && input.UserID != "1002" && input.UserID != "1003") {
		c.JSON(400, gin.H{"error": "请选择有效测试用户"})
		return
	}
	c.SetSameSite(http.SameSiteStrictMode)
	c.SetCookie("likeflow_session", h.Auth.Token(input.UserID), 43200, "/", "", c.Request.TLS != nil, true)
	c.JSON(200, gin.H{"user_id": input.UserID})
}

func (h *Handler) Articles(c *gin.Context) {
	targets, err := h.Service.DB.Targets(c.Request.Context())
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(200, gin.H{"targets": targets, "demo_mode": h.Demo})
}

func (h *Handler) State(c *gin.Context) { h.state(c, "QUERY") }

func (h *Handler) Change(c *gin.Context) {
	var body struct {
		Action string `json:"action"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || (body.Action != "LIKE" && body.Action != "UNLIKE") {
		c.JSON(400, gin.H{"error": "action 必须为 LIKE 或 UNLIKE", "code": "INVALID_ARGUMENT"})
		return
	}
	h.state(c, body.Action)
}

func (h *Handler) state(c *gin.Context, action string) {
	typ, id, ok := parse(c)
	if !ok {
		return
	}
	state, err := h.Service.State(c.Request.Context(), typ, id, c.GetString("user_id"), action)
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(200, state)
}

func parse(c *gin.Context) (int, string, bool) {
	typ, err := strconv.Atoi(c.Param("type"))
	id := c.Param("id")
	_, idErr := model.Decimal(id, ^uint64(0), true)
	if err != nil || typ < 1 || typ > 3 || strconv.Itoa(typ) != c.Param("type") || idErr != nil {
		c.JSON(400, gin.H{"error": "无效对象类型或 ID", "code": "INVALID_ARGUMENT"})
		return 0, "", false
	}
	return typ, id, true
}

// Sync exposes the persisted state separately, so the UI can show asynchronous convergence.
func (h *Handler) Sync(c *gin.Context) {
	typ, id, ok := parse(c)
	if !ok {
		return
	}
	t, err := h.Service.Target(c.Request.Context(), typ, id)
	if err != nil {
		h.fail(c, err)
		return
	}
	v, err := h.Service.DB.Relation(c.Request.Context(), t, c.GetString("user_id"))
	if err != nil {
		h.fail(c, err)
		return
	}
	var count string
	err = h.Service.DB.DB.QueryRowContext(c.Request.Context(), `SELECT like_count FROM like_count WHERE target_type=? AND target_id=?`, typ, id).Scan(&count)
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(200, gin.H{"liked": v.Liked, "count": count, "version": strconv.FormatUint(v.Version, 10)})
}

func (h *Handler) fail(c *gin.Context, err error) {
	if errors.Is(err, model.ErrNotFound) {
		c.JSON(404, gin.H{"error": "对象不存在或不可访问", "code": "NOT_FOUND"})
		return
	}
	slog.Error("API operation failed", "path", c.Request.URL.Path, "error", err)
	code := "UNAVAILABLE"
	message := "服务暂不可用，请稍后查询状态"
	if c.Request.Method == http.MethodPut {
		code = "RESULT_UNKNOWN"
		message = "操作结果未知，请查询当前状态；请求可能仍在处理中"
	}
	c.JSON(503, gin.H{"error": message, "code": code})
}
