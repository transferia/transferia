//go:build !disable_greenplum_provider

package gpfdistbin

const (
	defaultGpfdistBinPath = "/usr/bin/gpfdist"
)

type GpfdistParams struct {
	IsEnabled      bool   // IsEnabled shows that gpfdist connection is used instead of direct connections to segments.
	GpfdistBinPath string // Path to gpfdist executable.
	ServiceSchema  string // ServiceSchema is a name of schema used for creating temporary objects.
}

func (p *GpfdistParams) WithDefaults() {
	if p.IsEnabled && p.GpfdistBinPath == "" {
		p.GpfdistBinPath = defaultGpfdistBinPath
	}
}
