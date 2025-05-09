package service

import (
	"currency-service/client"
	"time"
	"fmt"
)

type ConverterService struct {
	api *client.ExchangeRateHostClient
}

func NewConverterService() *ConverterService {
	return &ConverterService{
		api: client.NewClient(),
	}
}

func (s *ConverterService) GetRate(from, to string, date time.Time) (float64, error) {
	return s.api.GetExchangeRate(from, to, date)
}

func (s *ConverterService) GetHistoricalRates(from, to string, start, end time.Time) (map[string]float64, error) {
	history := make(map[string]float64)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		rate, err := s.api.GetExchangeRate(from, to, d)
		if err != nil {
			return nil, err
		}
		history[d.Format("2006-01-02")] = rate
	}
	return history, nil
}

func (s *ConverterService) Convert(from, to string, amount float64, date time.Time) (float64, error) {
	rate, err := s.api.GetExchangeRate(from, to, date)
	if err != nil {
		return 0, err
	}
	return amount * rate, nil
}
