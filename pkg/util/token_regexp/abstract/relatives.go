package abstract

type RelativesImpl struct {
	parent   Op
	children []Op
}

func (r *RelativesImpl) SetParent(in Op) {
	r.parent = in
}

func (r *RelativesImpl) Parent() Op {
	return r.parent
}

func (r *RelativesImpl) SetChildren(in []Op) {
	r.children = in
}

func (r *RelativesImpl) Children() []Op {
	return r.children
}

func NewRelativesImpl() *RelativesImpl {
	return &RelativesImpl{
		parent:   nil,
		children: nil,
	}
}
