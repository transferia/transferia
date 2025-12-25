package abstract

type CapturingGroupResults struct {
	groups [][]*MatchedOp
}

func (r *CapturingGroupResults) GroupsNum() int {
	return len(r.groups)
}

func (r *CapturingGroupResults) GroupToText(originalStr string, index int) string {
	if index >= len(r.groups) {
		return ""
	}
	return ResolveMatchedOps(originalStr, r.groups[index])
}

func (r *CapturingGroupResults) GroupToNonEmptyTokensLower(index int, emptyTokenType int) []string {
	if index >= len(r.groups) {
		return nil
	}
	result := make([]string, 0)
	for _, t := range r.groups[index] {
		for _, token := range t.Tokens() {
			if token.AntlrToken.GetTokenType() == emptyTokenType {
				continue
			}
			result = append(result, token.AntlrToken.GetText())
		}
	}
	return result
}

func (r *CapturingGroupResults) GroupMatchedOpArr(index int) []*MatchedOp {
	if index >= len(r.groups) {
		return nil
	}
	return r.groups[index]
}

func NewCapturingGroupResults(in [][]*MatchedOp) *CapturingGroupResults {
	return &CapturingGroupResults{
		groups: in,
	}
}
