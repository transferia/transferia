package greenplum

type Host struct {
	Name string
	Port int
	Role Role
}

type Role string

const (
	RoleUndefined Role = "UNDEFINED"
	RoleMaster    Role = "MASTER"
	RoleReplica   Role = "REPLICA"
)
