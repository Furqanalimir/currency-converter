package handler

import (
	"currency-service/service"
	"currency-service/utils"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	converter *service.ConverterService
}

func NewHandler() *Handler {
	return &Handler{
		converter: service.NewConverterService(),
	}
}

func (h *Handler) ConvertHandler(c *gin.Context) {
	from := c.Query("from")
	to := c.Query("to")
	amountStr := c.Query("amount")
	dateStr := c.DefaultQuery("date", time.Now().Format("2006-01-02"))

	amount, err := strconv.ParseFloat(amountStr, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid amount"})
		return
	}

	date, err := utils.ValidateDate(dateStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.converter.Convert(from, to, amount, date)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"amount": result})
}

func (h *Handler) LatestHandler(c *gin.Context) {
	from := c.Query("from")
	to := c.Query("to")

	rate, err := h.converter.GetRate(from, to, time.Now())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"rate": rate})
}

func (h *Handler) HistoryHandler(c *gin.Context) {
	from := c.Query("from")
	to := c.Query("to")
	startStr := c.Query("start")
	endStr := c.Query("end")

	start, err := utils.ValidateDate(startStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid start date"})
		return
	}

	end, err := utils.ValidateDate(endStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid end date"})
		return
	}

	history, err := h.converter.GetHistoricalRates(from, to, start, end)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"history": history})
}
