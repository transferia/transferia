package resources

import (
	"github.com/transferia/transferia/cloud/dataplatform/ycloud"
	"github.com/transferia/transferia/cloud/iam/accessservice/client/go/cloudauth"
)

const (
	ResourceYdb = "ydb.database"
	// TODO: it's not real iam resource, remove after YDBREQUESTS-4559
	ResourceYdbPath                 = "ydb.database-path"
	ResourceNetwork                 = "vpc.network"
	ResourceCloud                   = "resource-manager.cloud"
	ResourceMdbClickHouseCluster    = "managed-clickhouse.cluster"
	ResourceMdbElasticsearchCluster = "managed-elasticsearch.cluster"
	ResourceMdbGreenplumCluster     = "managed-greenplum.cluster"
	ResourceMdbKafkaCluster         = "managed-kafka.cluster"
	ResourceMdbMongodbCluster       = "managed-mongodb.cluster"
	ResourceMdbMysqlCluster         = "managed-mysql.cluster"
	ResourceMdbOpensearchCluster    = "managed-opensearch.cluster"
	ResourceMdbPostgresqlCluster    = "managed-postgresql.cluster"
	ResourceTransfer                = "data-transfer.transfer"
	ResourceEndpoint                = "data-transfer.endpoint"
	ResourceFolder                  = "resource-manager.folder"
	ResourceSecurityGroup           = "vpc.securityGroup"
	ResourceSubnet                  = "vpc.subnet"
	ResourceServiceAccount          = "iam.serviceAccount"
	ResourceConnection              = "connection-manager.connection"
)

func Folder(id string) cloudauth.Resource {
	return cloudauth.ResourceFolder(id)
}

func Cloud(id string) cloudauth.Resource {
	return cloudauth.ResourceCloud(id)
}

func Connection(id string) cloudauth.Resource {
	return cloudauth.Resource{
		ID:   id,
		Type: ResourceConnection,
	}
}

func ServiceAccount(id string) cloudauth.Resource {
	return cloudauth.ResourceServiceAccount(id)
}

func Subnet(id string) cloudauth.Resource {
	return cloudauth.Resource{
		ID:   id,
		Type: ResourceSubnet,
	}
}

func SecurityGroup(id string) cloudauth.Resource {
	return cloudauth.Resource{
		ID:   id,
		Type: ResourceSecurityGroup,
	}
}

func Gizmo() cloudauth.Resource {
	return cloudauth.Resource{
		ID:   "gizmo",
		Type: ycloud.ResourceIAMGizmo,
	}
}

func DataTransferGizmo() cloudauth.Resource {
	return cloudauth.Resource{
		ID:   "data-transfer",
		Type: ycloud.ResourceIAMGizmo,
	}
}

func PostgreSQLCluster(id string) cloudauth.Resource {
	return cloudauth.Resource{
		ID:   id,
		Type: ResourceMdbPostgresqlCluster,
	}
}
