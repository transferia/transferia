package engines

type engineType string

type engine interface {
	IsEngine()
	String() string
}
