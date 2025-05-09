package utils

import (
	"github.com/robfig/cron/v3"
	"fmt"
)

var CronScheduler *cron.Cron

func InitCron() {
	CronScheduler = cron.New()
	CronScheduler.Start()
}


func AddCronJob(spec string, jobFunc func()) (cron.EntryID, error) {
	fmt.Println("Cron job scheduled:", spec)
	if CronScheduler == nil {
		InitCron()
	}
	return CronScheduler.AddFunc(spec, jobFunc)
}
