package utils

import (
	"errors"
	"time"
)

func ValidateDate(dateStr string) (time.Time, error) {
	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return time.Time{}, errors.New("invalid date format (expected YYYY-MM-DD)")
	}

	if time.Since(date) > 90*24*time.Hour {
		return time.Time{}, errors.New("date is beyond 90-day historical limit")
	}

	return date, nil
}
