package fgssa

func SetUseFineGrainedSSA(use bool) {
	useFineGrainedSSA = use
}

func UseFineGrainedSSA() bool {
	return useFineGrainedSSA
}

var useFineGrainedSSA bool
