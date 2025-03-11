package debezium

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

func kindToOp(kind abstract.Kind, snapshot bool, emitType emitType) (string, error) {
	if kind == abstract.InsertKind {
		if snapshot {
			return "r", nil
		} else {
			return "c", nil
		}
	} else if kind == abstract.UpdateKind {
		switch emitType {
		case regularEmitType:
			return "u", nil
		case deleteEventEmitType:
			return "d", nil
		case insertEventEmitType:
			return "c", nil
		default:
			return "", xerrors.Errorf("unsupported emitType: %d", emitType)
		}
	} else if kind == abstract.DeleteKind {
		return "d", nil
	} else {
		return "", xerrors.Errorf("unsupported kind: %s", kind)
	}
}

func opToKind(op string) (abstract.Kind, error) {
	switch op {
	case "c":
		return abstract.InsertKind, nil
	case "r":
		return abstract.InsertKind, nil
	case "u":
		return abstract.UpdateKind, nil
	case "d":
		return abstract.DeleteKind, nil
	default:
		return abstract.InsertKind, xerrors.Errorf("unknown op: %s", op)
	}
}
