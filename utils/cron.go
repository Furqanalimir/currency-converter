package utils

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"currency-service/client"
)

var CronScheduler *cron.Cron

func InitCron() {
	CronScheduler = cron.New()

	_, err := CronScheduler.AddFunc("*/30 * * * *", exchangeRateJob)
	if err != nil {
		fmt.Println("Failed to schedule job:", err)
	}

	CronScheduler.Start()
}

func exchangeRateJob() {
	date := time.Now()
	rate, err := client.GetExchangeRate("USD", "INR", date)
	if err != nil {
		return 0, err
	}
	return amount * rate, nil
}

func AddCronJob(spec string, jobFunc func()) (cron.EntryID, error) {
	if CronScheduler == nil {
		InitCron()
	}
	return CronScheduler.AddFunc(spec, jobFunc)
}
