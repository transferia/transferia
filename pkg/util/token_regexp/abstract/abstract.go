package abstract

type Op interface {
	Relatives
	IsOp()
}

type Relatives interface {
	SetParent(Op)
	Parent() Op
	SetChildren([]Op)
	Children() []Op
}

type OpPrimitive interface {
	Op

	// ConsumePrimitive
	//   returns array of lengths, which are matched by op
	//       for example, in usual regexps regexp `a+` can match in string "aa" - matched lengths here []int{1,2}
	//       here are the same - return all matched paths (their lengths)
	//   returns nil if not matched
	ConsumePrimitive(tokens []*Token) []int
}

type OpComplex interface {
	Op

	// ConsumeComplex
	//   returns *MatchedResults, which is matched by op
	//       for example, in usual regexps regexp `(?:ab|abc)` can match in string "abc" by two paths: "ab" & "abc"
	//       here are the same - return all matched paths
	ConsumeComplex(tokens []*Token) *MatchedResults
}

type IsCapturingGroup interface {
	IsCapturingGroup()
}
