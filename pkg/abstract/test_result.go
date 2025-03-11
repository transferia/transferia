package abstract

import (
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
)

// CheckType test check type
type CheckType string

// CheckResult describe particular check result that was performed against endpoint / transfer
type CheckResult struct {
	Error   error
	Success bool
}

// TestResult aggregated result of test for endpoint
type TestResult struct {
	Checks  map[CheckType]CheckResult
	Schema  TableMap
	Preview map[TableID][]ChangeItem
}

func (t *TestResult) Add(extraChecks ...CheckType) {
	for _, check := range extraChecks {
		t.Checks[check] = CheckResult{Error: nil, Success: false}
	}
}

// Combine combines the two checkResult maps into one
func (t *TestResult) Combine(partialResults *TestResult) {
	for checkType, checkVal := range partialResults.Checks {
		t.Checks[checkType] = checkVal
	}

	if t.Preview == nil {
		t.Preview = map[changeitem.TableID][]changeitem.ChangeItem{}
	}

	if t.Schema == nil {
		t.Schema = map[TableID]TableInfo{}
	}

	for tableID, items := range partialResults.Preview {
		t.Preview[tableID] = items
	}

	for tableID, tableINfo := range partialResults.Schema {
		t.Schema[tableID] = tableINfo
	}
}

func (t *TestResult) Ok(checkType CheckType) *TestResult {
	t.Checks[checkType] = CheckResult{Error: nil, Success: true}
	return t
}

func (t *TestResult) NotOk(checkType CheckType, err error) *TestResult {
	t.Checks[checkType] = CheckResult{Error: err, Success: err == nil}
	return t
}

func (t *TestResult) Err() error {
	var err error
	for _, check := range t.Checks {
		if !check.Success && check.Error != nil {
			err = multierr.Append(err, check.Error)
		}
	}
	return err
}

func NewTestResult(checks ...CheckType) *TestResult {
	c := map[CheckType]CheckResult{}
	for _, check := range checks {
		c[check] = CheckResult{Error: nil, Success: false}
	}
	return &TestResult{
		Checks:  c,
		Schema:  nil,
		Preview: nil,
	}
}
