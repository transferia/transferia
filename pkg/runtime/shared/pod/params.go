package pod

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

var (
	// SharedDir is for storing files on filesystem and share it with host and onther processes
	// must be in sync with:
	//
	//	https://github.com/transferia/transferia/arcadia/transfer_manager/datacloud/packages/data-plane-ami/filesystem/var/lib/data-transfer/pod-config-tmpl/dataplane-pod.tmpl.yaml?rev=r9900092#L15
	//	https://github.com/transferia/transferia/arcadia/transfer_manager/go/pkg/config/dataplane/installations/aws_prod.yaml?rev=9b7fee2762#L46
	//	https://github.com/transferia/transferia/arcadia/transfer_manager/go/pkg/config/dataplane/installations/aws_preprod.yaml?rev=9b7fee2762#L46
	//	https://github.com/transferia/transferia/arcadia/transfer_manager/ci/teamcity/build_compute_image/datatransfer-pod.tmpl.yaml?rev=r9243535#L13
	SharedDir = "/var/lib/data-transfer"

	CPUMin = resource.MustParse("100m")
	CPUMax = resource.MustParse("1500m")

	RAMMin = resource.MustParse("256M")
	RAMMax = resource.MustParse("4G")
)
