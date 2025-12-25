package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type JobContext struct {
	Meta struct {
		WorkflowUID         string `json:"workflowUid"`
		WorkflowInstanceUID string `json:"workflowInstanceUid"`
		WorkflowURL         string `json:"workflowURL"`
		OperationUID        string `json:"operationUid"`
		BlockUID            string `json:"blockUid"`
		BlockCode           string `json:"blockCode"`
		BlockURL            string `json:"blockURL"`
		ProcessUID          string `json:"processUid"`
		ProcessURL          string `json:"processURL"`
		Priority            struct {
			Min       int     `json:"min"`
			Max       int     `json:"max"`
			Value     int     `json:"value"`
			NormValue float64 `json:"normValue"`
		} `json:"priority"`
		Description  string `json:"description"`
		Owner        string `json:"owner"`
		QuotaProject string `json:"quotaProject"`
	} `json:"meta"`
	Parameters  Config      `json:"parameters"`
	Inputs      interface{} `json:"inputs"`
	Outputs     interface{} `json:"outputs"`
	InputItems  interface{} `json:"inputItems"`
	OutputItems interface{} `json:"outputItems"`
}

func TryParseNirvanaConfig() (*Config, error) {
	dat, err := os.ReadFile("job_context.json")
	if err != nil {
		return nil, err
	}
	context := new(JobContext)

	if err := json.Unmarshal(dat, context); err != nil {
		return nil, fmt.Errorf("not in Nirvana")
	}

	return &context.Parameters, nil
}
