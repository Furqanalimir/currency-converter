package utils

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

var CronScheduler *cron.Cron

// Initialize the cron scheduler
func InitCron() {
	CronScheduler = cron.New()

	// Example: Run a job every minute
	_, err := CronScheduler.AddFunc("* * * * *", exampleJob)
	if err != nil {
		fmt.Println("Failed to schedule example job:", err)
	}

	CronScheduler.Start()
}

// Example job function
func exampleJob() {
	fmt.Println("Running example job at", time.Now())
}

// Add a custom job with a schedule
func AddCronJob(spec string, jobFunc func()) (cron.EntryID, error) {
	if CronScheduler == nil {
		InitCron()
	}
	return CronScheduler.AddFunc(spec, jobFunc)
}
