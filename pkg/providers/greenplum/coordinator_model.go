//go:build !disable_greenplum_provider

package greenplum

type GreenplumHostPort struct {
	Host string `json:"host"`
	Port int64  `json:"port"`
}

func (p *GreenplumHostPort) GetHost() string {
	return p.Host
}

func (p *GreenplumHostPort) GetPort() int64 {
	return p.Port
}

type GreenplumHAPair struct {
	Mirror  *GreenplumHostPort `json:"mirror,omitempty"`
	Primary *GreenplumHostPort `json:"primary,omitempty"`
}

func (p *GreenplumHAPair) GetMirror() *GreenplumHostPort {
	return p.Mirror
}

func (p *GreenplumHAPair) GetPrimary() *GreenplumHostPort {
	return p.Primary
}

type GreenplumCluster struct {
	Coordinator *GreenplumHAPair   `json:"coordintor,omitempty"`
	Segments    []*GreenplumHAPair `json:"segments,omitempty"`
}

func (x *GreenplumCluster) GetCoordinator() *GreenplumHAPair {
	return x.Coordinator
}

func (x *GreenplumCluster) GetSegments() []*GreenplumHAPair {
	return x.Segments
}

type WorkersGpConfig struct {
	WtsList []*WorkerIDToGpSegs `json:"wtsList"`
	Cluster *GreenplumCluster
}

func (x *WorkersGpConfig) GetWtsList() []*WorkerIDToGpSegs {
	if x != nil {
		return x.WtsList
	}
	return nil
}

func (x *WorkersGpConfig) GetCluster() *GreenplumCluster {
	return x.Cluster
}

type WorkerIDToGpSegs struct {
	WorkerID int32          `json:"workerID,omitempty"`
	Segments []*GpSegAndXID `json:"segments,omitempty"`
}

func (x *WorkerIDToGpSegs) GetWorkerID() int32 {
	if x != nil {
		return x.WorkerID
	}
	return 0
}

func (x *WorkerIDToGpSegs) GetSegments() []*GpSegAndXID {
	if x != nil {
		return x.Segments
	}
	return nil
}

type GpSegAndXID struct {
	SegmentID int32 `json:"segmentID,omitempty"`
	Xid       int64 `json:"xid,omitempty"`
}

func (x *GpSegAndXID) GetSegmentID() int32 {
	if x != nil {
		return x.SegmentID
	}
	return 0
}

func (x *GpSegAndXID) GetXid() int64 {
	if x != nil {
		return x.Xid
	}
	return 0
}
