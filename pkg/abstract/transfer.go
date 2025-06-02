package abstract

type RuntimeStatus int

const (
	Unknown      = RuntimeStatus(0)
	Initializing = RuntimeStatus(1)
	Alive        = RuntimeStatus(2)
	UserError    = RuntimeStatus(3)
	ServerError  = RuntimeStatus(4)
	Stopping     = RuntimeStatus(5)
	NotOwned     = RuntimeStatus(6)
)

func (s RuntimeStatus) String() string {
	switch s {
	case Initializing:
		return "Initializing"
	case Alive:
		return "Alive"
	case UserError:
		return "UserError"
	case ServerError:
		return "ServerError"
	case Stopping:
		return "Stopping"
	case NotOwned:
		return "NotOwned"
	default:
		return "Unknown"
	}
}

var AllRuntimeStatuses = []RuntimeStatus{
	Unknown,
	Initializing,
	Alive,
	UserError,
	ServerError,
	Stopping,
	NotOwned,
}

type Transfer interface {
	// Start the transfer. Retry endlessly when errors occur, until stopped with Stop().
	// This method does not block, the work is done in the background.
	Start()

	// Stop the transfer. May be called multiple times even without prior Start() to clean
	// up external resources, e.g. terminate YT operations. Synchronous, i.e. blocks
	// until either all resources are released or an error occurrs.
	Stop() error

	Runtime() Runtime

	Error() error

	RuntimeStatus() RuntimeStatus
	// Stop monitoring transfer and forget it when new scheduler is overtaking it
	Detach()
}
