package coordinator

import "github.com/transferia/transferia/pkg/abstract/model"

// Editor is obsolete interface to update entities on coordinator
//
// Deprecated: do not rely on it anymore
type Editor interface {
	// GetEndpointTransfers get all *other* linked transfer to either source or target of provider *transferID*
	GetEndpointTransfers(transferID string, isSource bool) ([]*model.Transfer, error)
	// GetTransfers return *related* transfers to *transferID* in desired statuses
	GetTransfers(statuses []model.TransferStatus, transferID string) ([]*model.Transfer, error)
	// GetEndpoint get source or target *server.EndpointParams* for provided *transferID*
	GetEndpoint(transferID string, isSource bool) (model.EndpointParams, error)
	// UpdateEndpoint update *server.EndpointParams* for provided *transferID*
	UpdateEndpoint(transferID string, endpoint model.EndpointParams) error
}
