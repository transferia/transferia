package gpfdistbin

const (
	defaultGpfdistBinPath = "/usr/bin/gpfdist"
)

type GpfdistParams struct {
	IsEnabled      bool   // IsEnabled shows that gpfdist connection is used instead of direct connections to segments.
	GpfdistBinPath string // Path to gpfdist executable.
	ServiceSchema  string // ServiceSchema is a name of schema used for creating temporary objects.
	ThreadsCount   int
}

func NewGpfdistParams(path, schema string, threads int) GpfdistParams {
	if path == "" {
		path = defaultGpfdistBinPath
	}
	return GpfdistParams{
		IsEnabled:      true,
		GpfdistBinPath: path,
		ServiceSchema:  schema,
		ThreadsCount:   threads,
	}
}
