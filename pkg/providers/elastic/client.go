package elastic

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"slices"
	"unsafe"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/connection"
	"github.com/transferia/transferia/pkg/connection/opensearch"
	"github.com/transferia/transferia/pkg/dbaas"
	"go.ytsaurus.tech/library/go/core/log"
)

type ServerType int64

const (
	Undefined                = 0
	OpenSearch               = 1
	ElasticSearch ServerType = 2
)

func openSearchResolveHosts(clusterID string) ([]string, error) {
	hosts, err := dbaas.ResolveClusterHosts(dbaas.ProviderTypeOpenSearch, clusterID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get hosts for ClusterID, err: %w", err)
	}
	result := make([]string, 0)
	for _, currHost := range hosts {
		if currHost.Type == "OPENSEARCH" {
			result = append(result, fmt.Sprintf("https://%s", net.JoinHostPort(currHost.Name, "9200")))
		}
	}
	return result, nil
}

func configFromConnection(logger log.Logger, connectionID string) (*elasticsearch.Config, error) {
	connmanConnection, err := connection.Resolver().ResolveConnection(context.Background(), connectionID, "opensearch")
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve connection from connection ID: %s, err: %w", connectionID, err)
	}
	openSearchConnection, ok := connmanConnection.(*opensearch.Connection)
	if !ok {
		return nil, xerrors.Errorf("unable to cast connection to OpenSearchConnection, err: %w", err)
	}
	isMDBConnection := openSearchConnection.ClusterID != ""
	protocol := "http"
	if openSearchConnection.HasTLS || isMDBConnection {
		protocol = "https"
	}
	addresses := make([]string, 0)
	for _, currHost := range openSearchConnection.Hosts {
		// If it's not mdb connection, we need to add all hosts, for mdb connection we need to add only data nodes
		if !isMDBConnection || slices.Contains(currHost.Roles, opensearch.GroupRoleData) {
			addresses = append(addresses, fmt.Sprintf("%s://%s", protocol, net.JoinHostPort(currHost.Name, fmt.Sprintf("%d", currHost.Port))))
		}
	}
	if len(addresses) == 0 && isMDBConnection {
		return nil, xerrors.Errorf("no data nodes found in connection %s", connectionID)
	}
	if len(addresses) == 0 && !isMDBConnection {
		return nil, xerrors.Errorf("no hosts found in connection %s", connectionID)
	}
	logger.Info("Resolved OpenSearch hosts", log.String("connectionID", connectionID), log.Any("hosts", addresses))

	var caCert []byte
	if len(openSearchConnection.CACertificates) > 0 {
		caCert = []byte(openSearchConnection.CACertificates)
	}

	return &elasticsearch.Config{
		Addresses:            addresses,
		Username:             openSearchConnection.User,
		Password:             string(openSearchConnection.Password),
		CACert:               caCert,
		UseResponseCheckOnly: true,
	}, nil
}

func elasticSearchResolveHosts(clusterID string) ([]string, error) {
	hosts, err := dbaas.ResolveClusterHosts(dbaas.ProviderTypeElasticSearch, clusterID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get hosts for ClusterID, err: %w", err)
	}
	result := make([]string, 0)
	for _, currHost := range hosts {
		if currHost.Type == "DATA_NODE" {
			result = append(result, fmt.Sprintf("https://%s", net.JoinHostPort(currHost.Name, "9200")))
		}
	}
	return result, nil
}

func ConfigFromDestination(logger log.Logger, cfg *ElasticSearchDestination, serverType ServerType) (*elasticsearch.Config, error) {
	var useResponseCheckOnly bool
	addresses := make([]string, 0)
	var err error

	switch serverType {
	case OpenSearch:
		useResponseCheckOnly = true
		if cfg.ConnectionID != "" {
			return configFromConnection(logger, cfg.ConnectionID)
		}
		if cfg.ClusterID != "" {
			addresses, err = openSearchResolveHosts(cfg.ClusterID)
			if err != nil {
				return nil, xerrors.Errorf("unable to resolve hosts, err: %w", err)
			}
			logger.Info("Resolved OpenSearch hosts", log.String("clusterID", cfg.ClusterID), log.Any("hosts", addresses))
		}
	case ElasticSearch:
		useResponseCheckOnly = false
		if cfg.ClusterID != "" {
			addresses, err = elasticSearchResolveHosts(cfg.ClusterID)
			if err != nil {
				return nil, xerrors.Errorf("unable to resolve hosts, err: %w", err)
			}
			logger.Info("Resolved ElasticSearch hosts", log.String("clusterID", cfg.ClusterID), log.Any("hosts", addresses))
		}
	default:
		return nil, xerrors.Errorf("unknown ")
	}

	if cfg.ClusterID == "" {
		protocol := "http"
		if cfg.SSLEnabled {
			protocol = "https"
		}
		for _, el := range cfg.DataNodes {
			addresses = append(addresses, fmt.Sprintf("%s://%s:%d", protocol, el.Host, el.Port))
		}
	}
	logger.Info("addresses exposed", log.Any("addresses", addresses))

	var caCert []byte
	if len(cfg.TLSFile) > 0 {
		caCert = []byte(cfg.TLSFile)
	}

	return &elasticsearch.Config{
		Addresses:            addresses,
		Username:             cfg.User,
		Password:             string(cfg.Password),
		CACert:               caCert,
		UseResponseCheckOnly: useResponseCheckOnly,
	}, nil
}

// setProductCheckSuccess
// cures client from working-only-with-elastic
func setProductCheckSuccess(client *elasticsearch.Client) error {
	value := reflect.ValueOf(&client)
	elem := value.Elem()
	field := reflect.Indirect(elem).FieldByName("productCheckSuccess")
	if !field.IsValid() {
		return xerrors.New("unable to find field 'productCheckSuccess' in elastic client")
	}
	allowedPrivateField := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	allowedPrivateField.SetBool(true)
	return nil
}

func getResponseBody(res *esapi.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, xerrors.Errorf("unable to perform elastic request: %w", err)
	}
	if res.IsError() {
		return nil, xerrors.Errorf("failed elastic request, HTTP status: %s, err: %s", res.Status(), res.String())
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, xerrors.Errorf("failed to read response body: %w", err)
	}

	return body, nil
}

func WithLogger(config elasticsearch.Config, logger log.Logger, serverType ServerType) (*elasticsearch.Client, error) {
	config.Logger = &eslogger{logger}
	client, err := elasticsearch.NewClient(config)
	if err != nil {
		return nil, xerrors.Errorf("Unable to create client with logger: %w", err)
	}
	if serverType != ElasticSearch {
		err := setProductCheckSuccess(client)
		if err != nil {
			return nil, xerrors.Errorf("failed to set 'productCheckSuccess' field, err: %w", err)
		}
	}
	return client, nil
}
