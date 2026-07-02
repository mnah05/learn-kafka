package api

import (
	"encoding/json"
	"net/http"

	"kafka-learn/kafka-basic/internal/producer"

	"github.com/labstack/echo/v4"
)

type Handler struct {
	producer *producer.Producer
	topic    string
}

func New(p *producer.Producer, topic string) *Handler {
	return &Handler{producer: p, topic: topic}
}

func (h *Handler) Register(e *echo.Echo) {
	e.POST("/echo", h.postEcho)
	e.GET("/echo", h.getEcho)
}

type echoRequest struct {
	Message string `json:"message"`
}

type echoResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func (h *Handler) postEcho(c echo.Context) error {
	var req echoRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, echoResponse{Status: "error", Message: "bad request"})
	}

	data, _ := json.Marshal(req)
	if err := h.producer.Send(h.topic, 0, data); err != nil {
		return c.JSON(http.StatusInternalServerError, echoResponse{Status: "error", Message: "kafka send failed"})
	}

	return c.JSON(http.StatusOK, echoResponse{Status: "ok", Message: req.Message})
}

func (h *Handler) getEcho(c echo.Context) error {
	return c.JSON(http.StatusOK, echoResponse{Status: "ok", Message: "echo service running"})
}
