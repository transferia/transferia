package mongo

import (
	"sort"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

type MongoDestination struct {
	ClusterID         string
	Hosts             []string
	Port              int
	Database          string
	ReplicaSet        string
	AuthSource        string
	User              string
	Password          model.SecretString
	TransformerConfig map[string]string
	Cleanup           model.CleanupType
	SubNetworkID      string
	SecurityGroupIDs  []string
	TLSFile           string
	ConnectionID      string
	// make a `direct` connection to mongo, see: https://www.mongodb.com/docs/drivers/go/current/fundamentals/connections/connection-guide/
	Direct bool

	RootCAFiles []string
	// indicates whether the mongoDB client uses a mongodb+srv connection
	SRVMode bool
}

var _ model.Destination = (*MongoDestination)(nil)
var _ model.WithConnectionID = (*MongoDestination)(nil)

func (d *MongoDestination) MDBClusterID() string {
	return d.ClusterID
}

func (d *MongoDestination) WithDefaults() {
	if d.Port <= 0 {
		d.Port = 27018
	}
	if d.Cleanup == "" {
		d.Cleanup = model.Drop
	}
	if len(d.Hosts) > 1 {
		sort.Strings(d.Hosts)
	}
}

func (d *MongoDestination) CleanupMode() model.CleanupType {
	return d.Cleanup
}

func (d *MongoDestination) Transformer() map[string]string {
	return d.TransformerConfig
}

func (MongoDestination) IsDestination() {
}

func (d *MongoDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *MongoDestination) HasTLS() bool {
	return d.ClusterID != "" || d.TLSFile != ""
}

func (d *MongoDestination) ConnectionOptions(caCertPaths []string) MongoConnectionOptions {
	return d.ToStorageParams().ConnectionOptions(caCertPaths)
}

func (d *MongoDestination) Validate() error {
	return nil
}

func (d *MongoDestination) GetConnectionID() string {
	return d.ConnectionID
}

func (d *MongoDestination) ToStorageParams() *MongoStorageParams {
	return &MongoStorageParams{
		TLSFile:           d.TLSFile,
		ClusterID:         d.ClusterID,
		Hosts:             d.Hosts,
		Port:              d.Port,
		ReplicaSet:        d.ReplicaSet,
		AuthSource:        d.AuthSource,
		User:              d.User,
		Password:          string(d.Password),
		Collections:       make([]MongoCollection, 0),
		DesiredPartSize:   TablePartByteSize,
		PreventJSONRepack: false,
		Direct:            d.Direct,
		RootCAFiles:       d.RootCAFiles,
		SRVMode:           d.SRVMode,
		ConnectionID:      d.ConnectionID,
	}
}
