package gpfdistbin

const (
	defaultGpfdistBinPath = "/usr/bin/gpfdist"
)

type GpfdistParams struct {
	IsEnabled      bool   // IsEnabled shows that gpfdist connection is used instead of direct connections to segments.
	GpfdistBinPath string // Path to gpfdist executable.
	ServiceSchema  string // ServiceSchema is a name of schema used for creating temporary objects.
	Host           string // Host is for developers without full-qualified host name.
	PipesCnt       int    // PipesCnt is number of pipes used by each gpfdist binary.
}

func (p *GpfdistParams) WithDefaults() {
	if p.IsEnabled && p.GpfdistBinPath == "" {
		p.GpfdistBinPath = defaultGpfdistBinPath
	}
}
