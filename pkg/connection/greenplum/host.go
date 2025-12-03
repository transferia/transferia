package greenplum

type Host struct {
	Name string `log:"true"`
	Port int    `log:"true"`
	Role Role   `log:"true"`
}

type Role string

const (
	RoleUndefined Role = "UNDEFINED"
	RoleMaster    Role = "MASTER"
	RoleReplica   Role = "REPLICA"
)
