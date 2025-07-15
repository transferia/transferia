package mysqlrecipe

import (
	"context"
	"os"
	"strconv"

	default_mysql "github.com/go-sql-driver/mysql"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/providers/mysql"
)

func RecipeMysqlSource() *mysql.MysqlSource {
	PrepareContainer(context.Background())
	port, _ := strconv.Atoi(os.Getenv("RECIPE_MYSQL_PORT"))
	src := new(mysql.MysqlSource)
	src.Host = os.Getenv("RECIPE_MYSQL_HOST")
	src.User = os.Getenv("RECIPE_MYSQL_USER")
	src.Password = model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD"))
	src.Database = os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE")
	src.Port = port
	src.ServerID = 1
	src.WithDefaults()
	return src
}

type RecipeParams struct {
	prefix       string
	connectionID string
}

func newRecipeParams() *RecipeParams {
	return &RecipeParams{
		prefix:       "",
		connectionID: "",
	}
}

type RecipeOption func(pg *RecipeParams)

func WithPrefix(prefix string) RecipeOption {
	return func(pg *RecipeParams) {
		pg.prefix = prefix
	}
}

func WithConnectionID(connID string) RecipeOption {
	return func(pg *RecipeParams) {
		pg.connectionID = connID
	}
}

func RecipeMysqlTarget(options ...RecipeOption) *mysql.MysqlDestination {
	params := newRecipeParams()
	for _, option := range options {
		option(params)
	}

	PrepareContainer(context.Background())
	port, _ := strconv.Atoi(os.Getenv(params.prefix + "RECIPE_MYSQL_PORT"))
	v := new(mysql.MysqlDestination)
	v.Host = os.Getenv(params.prefix + "RECIPE_MYSQL_HOST")
	v.User = os.Getenv(params.prefix + "RECIPE_MYSQL_USER")
	v.Password = model.SecretString(os.Getenv(params.prefix + "RECIPE_MYSQL_PASSWORD"))
	v.Database = os.Getenv(params.prefix + "RECIPE_MYSQL_TARGET_DATABASE")
	v.Port = port
	v.SkipKeyChecks = false
	v.ConnectionID = params.connectionID
	v.WithDefaults()
	return v
}

func RecipeMysqlSourceWithConnection(connID string) (*mysql.MysqlSource, *connection.ConnectionMySQL) {
	port, _ := strconv.Atoi(os.Getenv("RECIPE_MYSQL_PORT"))
	database := os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE")
	src := new(mysql.MysqlSource)
	src.ServerID = 1
	src.Database = database
	src.ConnectionID = connID
	src.WithDefaults()

	managedConnection := ManagedConnection(port, os.Getenv("RECIPE_MYSQL_HOST"), database, os.Getenv("RECIPE_MYSQL_USER"), os.Getenv("RECIPE_MYSQL_PASSWORD"))
	return src, managedConnection
}

func RecipeMysqlTargetWithConnection(connID string, options ...RecipeOption) (*mysql.MysqlDestination, *connection.ConnectionMySQL) {
	params := newRecipeParams()
	for _, option := range options {
		option(params)
	}
	port, _ := strconv.Atoi(os.Getenv(params.prefix + "RECIPE_MYSQL_PORT"))
	database := os.Getenv(params.prefix + "RECIPE_MYSQL_TARGET_DATABASE")
	v := new(mysql.MysqlDestination)
	v.SkipKeyChecks = false
	v.Database = database
	v.ConnectionID = connID
	v.WithDefaults()

	managedConnection := ManagedConnection(port, os.Getenv("RECIPE_MYSQL_HOST"), database, os.Getenv("RECIPE_MYSQL_USER"), os.Getenv("RECIPE_MYSQL_PASSWORD"))
	return v, managedConnection
}

func ManagedConnection(port int, host, dbName, user, password string) *connection.ConnectionMySQL {
	return &connection.ConnectionMySQL{
		BaseSQLConnection: &connection.BaseSQLConnection{
			Hosts:          []*connection.Host{{Name: host, Port: port, Role: connection.RoleUnknown, ReplicaType: connection.ReplicaUndefined}},
			User:           user,
			Password:       model.SecretString(password),
			Database:       dbName,
			HasTLS:         false,
			CACertificates: "",
			ClusterID:      "",
		},
		DatabaseNames: nil,
	}
}

func WithMysqlInclude(src *mysql.MysqlSource, regex []string) *mysql.MysqlSource {
	src.IncludeTableRegex = regex
	return src
}

func Exec(query string, connectionParams *mysql.ConnectionParams) error {
	conn, err := mysql.Connect(connectionParams, func(config *default_mysql.Config) error {
		config.MultiStatements = true
		return nil
	})
	if err != nil {
		return xerrors.Errorf("unable to build connection: %w", err)
	}
	_, err = conn.Exec(query)
	if err != nil {
		return xerrors.Errorf("unable to exec: %s: %w", query, err)
	}
	return nil
}
