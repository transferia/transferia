package abstract

type MatchedPath struct {
	path         []*MatchedOp
	lengthTokens int
}

func (r *MatchedPath) Length() int {
	return r.lengthTokens
}

func (r *MatchedPath) Path() []*MatchedOp {
	return r.path
}

func (r *MatchedPath) Tokens() []*Token {
	result := make([]*Token, 0)
	for _, matchedOp := range r.path {
		result = append(result, matchedOp.Tokens()...)
	}
	return result
}

func (r *MatchedPath) CapturingGroups() *CapturingGroupResults {
	isParentMatched := func(checkingOp *MatchedOp, potentialParent Op) bool {
		currentOp := checkingOp.op
		for {
			if currentOp.Parent() == potentialParent {
				return true
			}
			currentOp = currentOp.Parent()
			if currentOp == nil {
				return false
			}
		}
	}

	result := make([][]*MatchedOp, 0, len(r.path))
	currBucket := make([]*MatchedOp, 0)
	var newCapturingGroupAddr Op = nil
	for _, el := range r.path {
		if newCapturingGroupAddr != nil && isParentMatched(el, newCapturingGroupAddr) {
			currBucket = append(currBucket, el)
		}
		if _, ok := el.op.(IsCapturingGroup); ok {
			// flush existing bucket
			if len(currBucket) != 0 {
				result = append(result, currBucket)
				currBucket = make([]*MatchedOp, 0)
			}
			currBucket = append(currBucket, el)
			newCapturingGroupAddr = el.op
		}
	}

	// flush existing bucket
	if len(currBucket) != 0 {
		result = append(result, currBucket)
	}

	return NewCapturingGroupResults(result)
}

// NewMatchedPathEmpty - ctor for empty_match
//
// empty_match means 'op' matched by zero tokens. It's useful for ops: '?', '*' - then can match zero tokens
func NewMatchedPathEmpty() *MatchedPath {
	return &MatchedPath{
		path:         nil,
		lengthTokens: 0,
	}
}

// NewMatchedPathPrimitive - ctor for 'ConsumePrimitive'
//
// 'ConsumePrimitive' matches 'op' to some amount of tokens
func NewMatchedPathPrimitive(matchedOp *MatchedOp) *MatchedPath {
	return &MatchedPath{
		path:         []*MatchedOp{matchedOp},
		lengthTokens: len(matchedOp.tokens),
	}
}

// NewMatchedPathParentOpChildPath - ctor for 'AddLocal'
//
// 'AddLocal' usually used after ConsumeComplex, to consume its results
func NewMatchedPathParentOpChildPath(parentOp *MatchedOp, childPath *MatchedPath) *MatchedPath {
	currPath := make([]*MatchedOp, 0, len(childPath.path)+1)
	currPath = append(currPath, parentOp)
	currPath = append(currPath, childPath.path...)
	return &MatchedPath{
		path:         currPath,
		lengthTokens: len(parentOp.tokens) + childPath.lengthTokens,
	}
}

// NewMatchedPathParentPathChildPath - ctor for cases, when need to concat parent matched path with child matched paths
//
//	used in 'plus'
func NewMatchedPathParentPathChildPath(parentPath *MatchedPath, childPath *MatchedPath) *MatchedPath {
	currPath := make([]*MatchedOp, 0, len(parentPath.path)+len(childPath.path))
	currPath = append(currPath, parentPath.path...)
	currPath = append(currPath, childPath.path...)
	return &MatchedPath{
		path:         currPath,
		lengthTokens: len(currPath),
	}
}
