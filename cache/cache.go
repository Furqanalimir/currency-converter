package cache

import (
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
)

type RateCache struct {
	internal *cache.Cache
}

var C *RateCache

func Init() error {
	C = &RateCache{
		internal: cache.New(1*time.Hour, 2*time.Hour), // itme expiry: 1h, cleanup: 2h
	}
	return nil
}

func (rc *RateCache) Set(key string, value interface{}, duration time.Duration) {
	rc.internal.Set(key, value, duration)
}

func (rc *RateCache) Get(key string) (interface{}, bool) {
	return rc.internal.Get(key)
}

func (rc *RateCache) Delete(key string) {
	rc.internal.Delete(key)
}

func GetRateKey(from, to string, date time.Time) string {
	return fmt.Sprintf("%s-%s-%s", from, to, date.Format("2006-01-02"))
}
