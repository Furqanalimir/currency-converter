package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"currency-service/cache"
	"currency-service/client"
	"currency-service/handler"
	"currency-service/utils"

	"github.com/gin-gonic/gin"
)

func main() {
	// Initialize application components
	if err := cache.Init(); err != nil {
		log.Fatalf("Failed to initialize cache: %v", err)
	}

	// Set up Gin router
	router := gin.Default()
	h := handler.NewHandler()

	// Define routes
	api := router.Group("/")
	{
		api.GET("convert", h.ConvertHandler)
		api.GET("latest", h.LatestHandler)
		api.GET("history", h.HistoryHandler)
	}

	// Create HTTP server
	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	utils.AddCronJob("*/30 * * * *", func() {
		fmt.Println("Custom job ran at", time.Now())
		c := client.NewClient()
		c.GetExchangeRate("USD", "INR", time.Now())
	})

	go func() {
		log.Println("Server running on http://localhost:8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Listen for OS signals for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit // Wait for signal

	log.Println("Shutting down server...")

	// Gracefully shut down with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited gracefully")
}
