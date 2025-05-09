package utils

import (
	"github.com/robfig/cron/v3"
)

var CronScheduler *cron.Cron

func InitCron() {
	CronScheduler = cron.New()
	CronScheduler.Start()
}


func AddCronJob(spec string, jobFunc func()) (cron.EntryID, error) {
	if CronScheduler == nil {
		InitCron()
	}
	return CronScheduler.AddFunc(spec, jobFunc)
}
