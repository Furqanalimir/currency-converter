package models

type ExchangeResponse struct {
	Info struct {
		Quote     float64 `json:"quote"`
		Timestamp int64   `json:"timestamp"`
	} `json:"info"`
	Privacy string `json:"privacy"`
	Query   struct {
		Amount float64 `json:"amount"`
		From   string  `json:"from"`
		To     string  `json:"to"`
	} `json:"query"`
	Result  float64 `json:"result"`
	Success bool    `json:"success"`
	Terms   string  `json:"terms"`
}
