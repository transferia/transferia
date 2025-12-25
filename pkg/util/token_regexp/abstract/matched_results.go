package abstract

type MatchedResults struct {
	paths []*MatchedPath
}

func (r *MatchedResults) AddMatchedPath(matchedPath *MatchedPath) {
	r.paths = append(r.paths, matchedPath)
}

func (r *MatchedResults) AddMatchedPathsAfterConsumePrimitive(lengths []int, op Op, allLeastTokens []*Token) {
	if lengths == nil {
		return
	}
	for _, currLength := range lengths {
		matchedOp := NewMatchedOp(op, allLeastTokens[0:currLength])
		r.paths = append(r.paths, NewMatchedPathPrimitive(matchedOp))
	}
}

// AddLocalResults - usually used after ConsumeComplex, to consume its results
func (r *MatchedResults) AddLocalResults(localResults *MatchedResults, op Op, opTokens []*Token) {
	if len(localResults.paths) == 0 {
		return
	}
	newPaths := make([]*MatchedPath, 0, len(localResults.paths))
	for _, path := range localResults.paths {
		matchedOp := NewMatchedOp(op, opTokens)
		newPaths = append(newPaths, NewMatchedPathParentOpChildPath(matchedOp, path))
	}
	r.paths = append(r.paths, newPaths...)
}

func (r *MatchedResults) AddAllPaths(collector *MatchedResults) {
	r.paths = append(r.paths, collector.paths...)
}

func (r *MatchedResults) Size() int {
	return len(r.paths)
}

func (r *MatchedResults) Index(index int) *MatchedPath {
	return r.paths[index]
}

func (r *MatchedResults) IsMatched() bool {
	return len(r.paths) != 0
}

func (r *MatchedResults) MatchedSubstring(originalStr string) string {
	if r == nil {
		return ""
	}
	if len(r.paths) == 0 {
		return ""
	}
	currPath := r.paths[0]
	return ResolveMatchedOps(originalStr, currPath.path)
}

func NewMatchedResults() *MatchedResults {
	return &MatchedResults{
		paths: make([]*MatchedPath, 0),
	}
}
