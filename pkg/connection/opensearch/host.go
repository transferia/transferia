package opensearch

type GroupRole string

const (
	GroupRoleUnspecified = GroupRole("UNSPECIFIED")
	GroupRoleData        = GroupRole("DATA")
	GroupRoleManager     = GroupRole("MANAGER")
)

type Host struct {
	Name  string      `log:"true"`
	Port  int         `log:"true"`
	Roles []GroupRole `log:"true"`
}
