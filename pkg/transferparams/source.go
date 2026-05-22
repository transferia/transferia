package transferparams

type Source interface {
	isTransferParamsSource()
}

var (
	_ Source = (*VmMetadataSource)(nil)
	_ Source = (*EnvironmentVariableSource)(nil)
)

type VmMetadataSource struct{}

type EnvironmentVariableSource struct {
	VariableName string
}

func (*VmMetadataSource) isTransferParamsSource() {}

func (*EnvironmentVariableSource) isTransferParamsSource() {}
