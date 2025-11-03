package gpfdistbin

const (
	defaultBinPath = "/usr/bin/gpfdist"
)

type GpfdistParams struct {
	IsEnabled      bool   // IsEnabled shows that gpfdist connection is used instead of direct connections to segments.
	GpfdistBinPath string // Path to gpfdist executable.
	ServiceSchema  string // ServiceSchema is a name of schema used for creating temporary objects.
	ThreadsCount   int
}

func NewGpfdistParams(binPath, serviceSchema string, threads int) *GpfdistParams {
	if binPath == "" {
		binPath = defaultBinPath
	}
	return &GpfdistParams{
		IsEnabled:      true,
		GpfdistBinPath: binPath,
		ServiceSchema:  serviceSchema,
		ThreadsCount:   threads,
	}
}
