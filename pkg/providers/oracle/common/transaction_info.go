package common

import (
	"fmt"

	"github.com/transferia/transferia/pkg/abstract2"
)

type TransactionInfo struct {
	id    string
	begin *LogPosition
	end   *LogPosition
}

func NewTransactionInfo(id string, begin *LogPosition, end *LogPosition) *TransactionInfo {
	return &TransactionInfo{
		id:    id,
		begin: begin,
		end:   end,
	}
}

func (info *TransactionInfo) ID() string {
	return info.id
}

func (info *TransactionInfo) OracleBeginPosition() *LogPosition {
	return info.begin
}

func (info *TransactionInfo) OracleEndPosition() *LogPosition {
	return info.end
}

// abstract2.Transaction

func (info *TransactionInfo) BeginPosition() abstract2.LogPosition {
	return info.begin
}

func (info *TransactionInfo) EndPosition() abstract2.LogPosition {
	return info.end
}

func (info *TransactionInfo) Equals(otherTransaction abstract2.Transaction) bool {
	if otherOracleTransaction, ok := otherTransaction.(*TransactionInfo); !ok {
		return false
	} else {
		return info.ID() == otherOracleTransaction.ID()
	}
}

func (info *TransactionInfo) String() string {
	return fmt.Sprintf("%v: From [%v], To [%v]", info.id, info.begin, info.end)
}
