package functions

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/credentials"
	"github.com/transferia/transferia/pkg/format"
	"go.ytsaurus.tech/library/go/core/log"
)

type Executor struct {
	eventSource EventSource
	cfg         *model.DataTransformOptions
	logger      log.Logger
	registry    metrics.Registry
	httpClient  *http.Client
	url         string
	creds       credentials.Credentials
}

type (
	EventSource   string
	ProcessResult string
)

const (
	YDS = EventSource("YDS")
	CDC = EventSource("CDC")

	OK               = ProcessResult("Ok")
	Split            = ProcessResult("Split")
	Dropped          = ProcessResult("Dropped")
	ProcessingFailed = ProcessResult("ProcessingFailed")
)

type Record struct {
	CDC               *CDCPayload   `json:"cdc"`
	CDCSplit          []CDCPayload  `json:"cdc_split"`
	YDS               *YDSPayload   `json:"kinesis"` // maybe YDS?
	Result            ProcessResult `json:"result"`
	EventSource       EventSource   `json:"eventSource"`
	EventID           string        `json:"eventID"`
	InvokeIdentityArn string        `json:"invokeIdentityArn"`
	EventVersion      string        `json:"eventVersion"`
	EventName         string        `json:"eventName"`
	EventSourceARN    string        `json:"eventSourceARN"`
	AwsRegion         string        `json:"awsRegion"`
}

type CDCPayload struct {
	// TODO: Fill with generic CDC event?
	abstract.ChangeItem
}

type YDSPayload struct {
	PartitionKey string  `json:"partitionKey"`
	Data         string  `json:"data"`
	SeqNo        uint64  `json:"sequenceNumber"`
	WriteTime    float64 `json:"approximateArrivalTimestamp"`
}

type Data struct {
	Records []Record `json:"Records"`
}

type cfBatch struct {
	left, right int
	data        *bytes.Buffer
}

func (e *Executor) Do(data []abstract.ChangeItem) ([]abstract.ChangeItem, error) {
	reqData := Data{Records: make([]Record, len(data))}
	switch e.eventSource {
	case CDC: // just a draft
		for i, r := range data {
			reqData.Records[i] = Record{
				CDC:               &CDCPayload{ChangeItem: r},
				CDCSplit:          nil,
				YDS:               nil,
				Result:            "",
				EventSource:       CDC,
				EventID:           fmt.Sprintf("%v.%v", r.LSN, r.Counter),
				InvokeIdentityArn: "tm", // SA ID?
				EventVersion:      "1.0",
				EventName:         "dt:cdc:change_item",
				EventSourceARN:    "",
				AwsRegion:         "",
			}
		}
	case YDS:
		// we sure regards to data format
		for i, r := range data {
			rawData, err := abstract.GetRawMessageData(r)
			if err != nil {
				return nil, xerrors.Errorf("unable to get raw message: %w", err)
			}
			reqData.Records[i] = Record{
				CDC:      nil,
				CDCSplit: nil,
				YDS: &YDSPayload{
					PartitionKey: fmt.Sprintf("%v-%v", r.ColumnValues[0], r.ColumnValues[1]),
					Data:         base64.StdEncoding.EncodeToString(rawData),
					SeqNo:        r.LSN,
					WriteTime:    float64(r.CommitTime / uint64(time.Second)),
				},
				Result:            "",
				EventSource:       YDS,
				EventID:           fmt.Sprintf("%v.%v", r.LSN, r.Counter),
				InvokeIdentityArn: "tm", // SA ID?
				EventVersion:      "1.0",
				EventName:         "dt:yds:record",
				EventSourceARN:    "",
				AwsRegion:         "",
			}
		}
	default:
		return nil, abstract.NewFatalError(xerrors.Errorf("not implemented event source: %v", e.eventSource))
	}
	var batches []*cfBatch
	currentBatch := &cfBatch{
		left:  0,
		right: 0,
		data:  new(bytes.Buffer),
	}
	totalSize := 0
	currentBatch.data.Write([]byte("{\"Records\":[")) // header
	for i, item := range reqData.Records {
		jsonItem, err := json.Marshal(item)
		if err != nil {
			return nil, xerrors.Errorf("failed to marshal a record: %w", err)
		}
		if currentBatch.data.Len() == 0 || currentBatch.data.Len() < int(e.cfg.BufferSize) {
			if currentBatch.left != i {
				currentBatch.data.Write([]byte(","))
			}
			currentBatch.data.Write(jsonItem)
		} else {
			currentBatch.right = i - 1
			currentBatch.data.Write([]byte("]}")) // footer
			batches = append(batches, currentBatch)
			totalSize += currentBatch.data.Len()
			currentBatch = &cfBatch{
				left:  i,
				right: i,
				data:  new(bytes.Buffer),
			}
			currentBatch.data.Write([]byte("{\"Records\":[")) // header
			currentBatch.data.Write(jsonItem)
		}
	}
	currentBatch.right = len(reqData.Records) - 1
	currentBatch.data.Write([]byte("]}")) // footer
	totalSize += currentBatch.data.Len()
	batches = append(batches, currentBatch)
	resData := Data{Records: make([]Record, len(data))}
	wg := sync.WaitGroup{}
	wg.Add(len(batches))
	errCh := make(chan error, len(batches))
	var iam string
	if e.creds != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		res, err := e.creds.Token(ctx)
		if err != nil {
			return nil, xerrors.Errorf("failed to obtain credentials: %w", err)
		}
		iam = res
	}
	e.logger.Infof("done serialize into batches: %v size %v (total %v)", len(batches), format.SizeInt(int(e.cfg.BufferSize)), format.SizeInt(totalSize))
	for _, batch := range batches {
		go func(batch *cfBatch) {
			defer wg.Done()
			if err := backoff.Retry(func() error {
				var req *http.Request
				r, err := http.NewRequest(http.MethodPost, e.url, batch.data)
				if err != nil {
					e.logger.Error("failed to prepare request function", log.Error(err))
					return xerrors.Errorf("failed to prepare request function: %w", err)
				}
				req = r
				req.Header.Set("Authorization", fmt.Sprintf("Bearer %v", iam))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Accept", "application/json")
				for _, header := range e.cfg.Headers {
					req.Header.Set(header.Key, header.Value)
				}
				resp, err := e.httpClient.Do(req)
				if err != nil {
					e.logger.Error("failed to invoke function", log.Error(err))
					return xerrors.Errorf("failed to invoke function: %w", err)
				}
				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					var errorResp interface{}
					if err = json.NewDecoder(resp.Body).Decode(&errorResp); err == nil {
						e.logger.Warn(
							"Failed to process request",
							log.Any("url", req.URL.String()),
							log.Any("resp", errorResp),
							log.Any("status", resp.Status),
						)
					}
					respStr, _ := json.MarshalIndent(errorResp, "", "\t")
					return xerrors.Errorf("Bad status code: %v, error: \n%v", resp.Status, string(respStr))
				}
				data, err := io.ReadAll(resp.Body)
				if err != nil {
					e.logger.Error("failed to read response body", log.Error(err))
					return xerrors.Errorf("failed to read response body: %w", err)
				}
				var batchRes Data
				if err = json.Unmarshal(data, &batchRes); err != nil {
					e.logger.Error(
						"Failed to parse response",
						log.Any("url", req.URL.String()),
						log.Any("status", resp.Status),
						log.Error(err),
					)
					return xerrors.Errorf("failed to parse response from %q (%s): %w", req.URL.String(), resp.Status, err)
				}
				if len(batchRes.Records) != (batch.right-batch.left)+1 {
					return xerrors.Errorf("records count differ: %v requested but %v received", (batch.right-batch.left)+1, len(batchRes.Records))
				}
				for i, record := range batchRes.Records {
					resData.Records[batch.left+i] = record
				}
				return nil
			}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(e.cfg.NumberOfRetries))); err != nil {
				errCh <- err
			}
		}(batch)
	}
	wg.Wait()
	close(errCh)
	var errors []error
	for err := range errCh {
		errors = append(errors, err)
	}
	if len(errors) != 0 {
		err := xerrors.New("")
		for _, childErr := range errors {
			err = xerrors.Errorf("%v\n%w", err, childErr)
		}
		return nil, xerrors.Errorf("failed to process message:\n%w", err)
	}
	if len(resData.Records) != len(reqData.Records) {
		return nil, xerrors.Errorf("records count differ: %v requested but %v received", len(reqData.Records), len(resData.Records))
	}
	var processed []abstract.ChangeItem
	for i, r := range resData.Records {
		switch e.eventSource {
		case CDC:
			switch r.Result {
			case Split:
				for _, split := range r.CDCSplit {
					processed = append(processed, split.ChangeItem)
				}
			case OK:
				processed = append(processed, r.CDC.ChangeItem)
			case Dropped:
				e.logger.Debugf("item dropped: %v", r.EventID)
				continue
			}
		case YDS:
			switch r.Result {
			case OK:
				processedData, err := base64.StdEncoding.DecodeString(r.YDS.Data)
				if err != nil {
					return nil, err
				}
				data[i].ColumnValues[4] = string(processedData)
				processed = append(processed, data[i])
			case Dropped:
				e.logger.Debugf("item dropped: %v", r.EventID)
				continue
			default:
				processed = append(processed, abstract.ChangeItem{
					ID:           data[i].ID,
					LSN:          data[i].LSN,
					CommitTime:   data[i].CommitTime,
					Counter:      data[i].Counter,
					Kind:         data[i].Kind,
					Schema:       data[i].Schema,
					Table:        fmt.Sprintf("%v_%v", data[i].Table, r.Result),
					PartID:       data[i].PartID,
					ColumnNames:  data[i].ColumnNames,
					ColumnValues: data[i].ColumnValues,
					TableSchema:  data[i].TableSchema,
					OldKeys:      *new(abstract.OldKeysType),
					TxID:         "",
					Query:        "",
					Size:         data[i].Size,
				})
			}
		default:
			return nil, abstract.NewFatalError(xerrors.Errorf("not implemented event source: %v", e.eventSource))
		}
	}
	return processed, nil
}

func NewExecutor(cfg *model.DataTransformOptions, baseURL string, source EventSource, lgr log.Logger, registry metrics.Registry) (*Executor, error) {
	var creds credentials.Credentials
	if cfg.ServiceAccountID != "" {
		serviceAccountCreds, err := credentials.NewServiceAccountCreds(lgr, cfg.ServiceAccountID)
		if err != nil {
			return nil, err
		}
		creds = serviceAccountCreds
	}
	return &Executor{
		cfg:         cfg,
		url:         fmt.Sprintf("%v/%v?integration=raw", baseURL, cfg.CloudFunction),
		eventSource: source,
		logger:      lgr,
		registry:    registry,
		httpClient: &http.Client{
			Timeout: cfg.InvocationTimeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return xerrors.Errorf("redirect to %s is requested but no redirects are allowed", req.URL.String())
			},
		},
		creds: creds,
	}, nil
}
