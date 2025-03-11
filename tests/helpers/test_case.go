package helpers

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
)

type TestCase interface {
	TableName() string
	Initialize(t *testing.T)
	ExecStatement(ctx context.Context, t *testing.T, client *pgxpool.Pool)
	AddChangeItem(in *abstract.ChangeItem)
	IsEnoughChangeItems() bool
	Check(t *testing.T)
}

//---

type TestCaseContainer struct {
	mutex          sync.Mutex
	tableNameToObj map[string]TestCase
}

func (c *TestCaseContainer) AddCase(in TestCase) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.tableNameToObj[in.TableName()] = in
}

func (c *TestCaseContainer) Initialize(t *testing.T) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, v := range c.tableNameToObj {
		v.Initialize(t)
	}
}

func (c *TestCaseContainer) ExecStatement(ctx context.Context, t *testing.T, client *pgxpool.Pool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, v := range c.tableNameToObj {
		v.ExecStatement(ctx, t, client)
	}
}

func (c *TestCaseContainer) AddChangeItem(t *testing.T, in *abstract.ChangeItem) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	logger.Log.Infof("AddChangeItem, changeItem: %s", in.ToJSONString())

	obj, ok := c.tableNameToObj[in.TableID().Name]
	require.True(t, ok)
	obj.AddChangeItem(in)
}

func (c *TestCaseContainer) IsEnoughChangeItems(t *testing.T) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for k, v := range c.tableNameToObj {
		isEnough := v.IsEnoughChangeItems()
		if !isEnough {
			logger.Log.Infof("testCase %v has not enough changeItems", k)
			return false
		}
	}
	return true
}

func (c *TestCaseContainer) Check(t *testing.T) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, v := range c.tableNameToObj {
		v.Check(t)
	}
}

func NewTestCaseContainer() *TestCaseContainer {
	return &TestCaseContainer{
		tableNameToObj: make(map[string]TestCase),
	}
}
