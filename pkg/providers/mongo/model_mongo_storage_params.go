package mongo

type MongoStorageParams struct {
	TLSFile           string
	ClusterID         string
	Hosts             []string
	Port              int
	ReplicaSet        string
	AuthSource        string
	User              string
	Password          string
	Collections       []MongoCollection
	DesiredPartSize   uint64
	PreventJSONRepack bool
	Direct            bool
	RootCAFiles       []string
	SRVMode           bool
	ConnectionID      string
}

func (s *MongoStorageParams) ConnectionOptions(defaultCACertPaths []string) MongoConnectionOptions {
	var caCert TrustedCACertificate
	if s.TLSFile != "" {
		caCert = InlineCACertificatePEM(s.TLSFile)
	} else if s.ClusterID != "" {
		caCert = CACertificatePEMFilePaths(defaultCACertPaths)
	}

	hosts := make([]HostWithPort, 0, len(s.Hosts))
	for _, host := range s.Hosts {
		hosts = append(hosts, HostWithPort{
			Host: host,
			Port: s.Port,
		})
	}

	return MongoConnectionOptions{
		ClusterID:     s.ClusterID,
		HostsWithPort: hosts,
		ReplicaSet:    s.ReplicaSet,
		AuthSource:    s.AuthSource,
		User:          s.User,
		Password:      s.Password,
		CACert:        caCert,
		Direct:        s.Direct,
		SRVMode:       s.SRVMode,
		ConnectionID:  s.ConnectionID,
	}
}
