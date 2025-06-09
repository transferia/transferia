package opensearch

type GroupRole string

const (
	GroupRoleUnspecified = GroupRole("UNSPECIFIED")
	GroupRoleData        = GroupRole("DATA")
	GroupRoleManager     = GroupRole("MANAGER")
)

type Host struct {
	Name  string
	Port  int
	Roles []GroupRole
}
