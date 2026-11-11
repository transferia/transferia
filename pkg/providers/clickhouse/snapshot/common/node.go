package ch_eo_common

import (
	"database/sql"

	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
)

type Node struct {
	Host *conn_clickhouse.Host
	DB   *sql.DB
	HTTP httpclient.HTTPClient
}

func NewNode(host *conn_clickhouse.Host, db *sql.DB, http httpclient.HTTPClient) Node {
	return Node{
		Host: host,
		DB:   db,
		HTTP: http,
	}
}
