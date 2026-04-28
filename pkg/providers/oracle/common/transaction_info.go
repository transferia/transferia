package common

import "fmt"

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

func (info *TransactionInfo) String() string {
	return fmt.Sprintf("%v: From [%v], To [%v]", info.id, info.begin, info.end)
}
