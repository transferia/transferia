package coded

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/set"
)

// Code define provider defined stable code. Each provider has own code-registry, but we have global registry to dedup them
// in case we have duplicate we will panic at start
type Code string

func (c Code) ID() string {
	return string(c)
}

func (c Code) Contains(err error) bool {
	var codedErr CodedError
	unwrappedErr := err
	for xerrors.As(unwrappedErr, &codedErr) {
		if codedErr.Code() == c {
			return true
		}
		unwrappedErr = xerrors.Unwrap(codedErr)
	}
	return false
}

var knownCodes = set.New[Code]()
var codeLinks = make(map[Code][]LinkData)
var codeDescriptions = make(map[Code]string)

// LinkData содержит информацию о ссылке на документацию
type LinkData struct {
	Text string
	URL  string
}

func Register(parts ...string) Code {
	code := Code(strings.Join(parts, "."))
	if knownCodes.Contains(code) {
		panic(fmt.Sprintf("code: %s already registered", code))
	}
	knownCodes.Add(code)
	return code
}

func RegisterLinkWithText(code Code, text, link string) {
	if !knownCodes.Contains(code) {
		panic(fmt.Sprintf("code: %s not registered, cannot register link", code))
	}
	codeLinks[code] = append(codeLinks[code], LinkData{Text: text, URL: link})
}

func RegisterShortDescription(code Code, description string) {
	if !knownCodes.Contains(code) {
		panic(fmt.Sprintf("code: %s not registered, cannot register description", code))
	}
	codeDescriptions[code] = description
}

func GetLinks(code Code) ([]LinkData, bool) {
	links, exists := codeLinks[code]
	return links, exists
}

func GetShortDescription(code Code) (string, bool) {
	description, exists := codeDescriptions[code]
	return description, exists
}

func All() []Code {
	return knownCodes.SortedSliceFunc(func(a, b Code) bool {
		return a > b
	})
}
