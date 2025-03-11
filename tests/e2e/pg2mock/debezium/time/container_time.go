package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
)

type containerTime struct {
	changeItems []abstract.ChangeItem
}

func (c *containerTime) TableName() string {
	return "table_with_timestamp"
}

func (c *containerTime) Initialize(t *testing.T) {
}

func (c *containerTime) ExecStatement(ctx context.Context, t *testing.T, client *pgxpool.Pool) {
}

func (c *containerTime) AddChangeItem(in *abstract.ChangeItem) {
	c.changeItems = append(c.changeItems, *in)
}

func (c *containerTime) IsEnoughChangeItems() bool {
	return len(c.changeItems) == 5
}

func (c *containerTime) Check(t *testing.T) {
	emitterWithOriginalTypes, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)

	changeItem := &c.changeItems[2]

	fmt.Printf("timmyb32rQQQ:%T", changeItem.ColumnValues[1])

	msgs, err := emitterWithOriginalTypes.EmitKV(changeItem, time.Time{}, true, nil)
	require.NoError(t, err)
	for _, msg := range msgs {
		fmt.Println("DEBEZIUM KEY", msg.DebeziumKey)
		fmt.Println("DEBEZIUM VAL", *(msg.DebeziumVal))
	}
}

func newContainerTime() *containerTime {
	return &containerTime{
		changeItems: make([]abstract.ChangeItem, 0),
	}
}
