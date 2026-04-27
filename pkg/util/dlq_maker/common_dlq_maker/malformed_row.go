package common_dlq_maker

type MalformedRow struct {
	Row   []byte `json:"row"`
	Error string `json:"error"`
}
