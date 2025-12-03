package mongo

type MongoStorageParams struct {
	TLSFile           string
	ClusterID         string   `log:"true"`
	Hosts             []string `log:"true"`
	Port              int      `log:"true"`
	ReplicaSet        string   `log:"true"`
	AuthSource        string   `log:"true"`
	User              string   `log:"true"`
	Password          string
	Collections       []MongoCollection `log:"true"`
	DesiredPartSize   uint64            `log:"true"`
	PreventJSONRepack bool              `log:"true"`
	Direct            bool              `log:"true"`
	RootCAFiles       []string
	SRVMode           bool   `log:"true"`
	ConnectionID      string `log:"true"`
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
