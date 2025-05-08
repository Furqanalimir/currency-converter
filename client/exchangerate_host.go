package client

import (
	"currency-service/cache"
	"currency-service/models"
	"currency-service/utils"
	"errors"

	gocache "github.com/patrickmn/go-cache"

	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type ExchangeRateHostClient struct{}

func NewClient() *ExchangeRateHostClient {
	return &ExchangeRateHostClient{}
}

func (c *ExchangeRateHostClient) GetExchangeRate(from, to string, date time.Time) (float64, error) {
	key := cache.GetRateKey(from, to, date)
	cached, found := cache.C.Get(key)
	if found && cached.(float64) != 0 {
		return cached.(float64), nil
	}

	// formattedDate := date.Format("2006-01-02") // YYYY-MM-DD
	apiKey, err := utils.GetRequiredEnv("EXCHANGE_API_SECRET_KEY")
	if err != nil {
		return 0, err
	}

	url := fmt.Sprintf("https://api.exchangerate.host/convert?access_key=%s&from=%s&to=%s&amount=%d", apiKey, from, to, 100)
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var exchangeResponse models.ExchangeResponse

	if err := json.NewDecoder(resp.Body).Decode(&exchangeResponse); err != nil {
		return 0, err
	}
	if !exchangeResponse.Success {
		return 0, errors.New("could not get conversion rate please try after sometime")
	}
	rate := exchangeResponse.Result
	cache.C.Set(key, rate, gocache.DefaultExpiration)
	return rate, nil
}
