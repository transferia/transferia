package binurl

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/blang/semver/v4"
	yslices "github.com/transferia/transferia/library/go/slices"
)

// BinaryLinks represent MongoDB binary distribution.
type BinaryLinks struct {
	URL          string
	Tag          string
	Arch         Arch
	OS           OperationSystem
	Distributive string
	Version      semver.Version
}

const (
	MongodbArchiveURL      = "https://www.mongodb.com/download-center/community/releases/archive"
	NotSpaceNorHyphenRegex = `[^ \t\r\n\f-]*`
	// TagProtoRegexp capture group comments:
	// #0: Tag
	// #1: OS
	// #2: Arch
	// #3: reserved
	// #4: Distributive (if any)
	// #5: Version
	TagProtoRegexp = `mongodb-(%s)-(%s)(-(%s))?-(%s)`
	// HTMLAProtoRegexp capture group comments:
	// #0: HTML <a> tag
	// #1: URL
	// #2: Tag
	// #3: OS
	// #4: Arch
	// #5: reserved
	// #6: Distributive (if any)
	// #7: Version
	HTMLAProtoRegexp = `<a href="(https:\/\/fastdl\.mongodb\.org\/\S+\.tgz)">(` + TagProtoRegexp + `\.tgz)<\/a>`
	// URLProtoRegexp capture group comments:
	// #0: URL
	// #1: Tag
	// #2: OS
	// #3: Arch
	// #4: reserved
	// #5: Distributive (if any)
	// #6: Version
	URLProtoRegexp = `https:\/\/fastdl\.mongodb\.org\/\S+\/(` + TagProtoRegexp + `)\.tgz`
)

var (
	TagRegexp   = buildRegexp(TagProtoRegexp)
	HTMLARegexp = buildRegexp(HTMLAProtoRegexp)
	URLRegexp   = buildRegexp(URLProtoRegexp)
)

func buildRegexp(tmpl string) *regexp.Regexp {
	opSystems := yslices.Map(OperationSystems, func(op OperationSystem) string { return string(op) })
	archs := yslices.Map(Archs, func(op Arch) string { return string(op) })

	re := fmt.Sprintf(tmpl,
		strings.Join(opSystems, "|"), // capture group #3
		strings.Join(archs, "|"),     // capture group #4
		NotSpaceNorHyphenRegex,       // capture group #6
		NotSpaceNorHyphenRegex,       // capture group #7
	)
	return regexp.MustCompile(re)
}

func MakeBinaryLinkByHTMLA(HTMLA string) *BinaryLinks {
	submatch := HTMLARegexp.FindStringSubmatch(HTMLA)
	if submatch == nil {
		return nil
	}
	version, err := semver.ParseTolerant(submatch[7])
	if err != nil {
		version = semver.Version{}
	}
	return &BinaryLinks{
		URL:          submatch[1],
		Tag:          submatch[2],
		OS:           ToOs(submatch[3]),
		Arch:         ToArch(submatch[4]),
		Distributive: submatch[6],
		Version:      version,
	}
}

func MakeBinaryLinkByURL(URL string) *BinaryLinks {
	submatch := URLRegexp.FindStringSubmatch(URL)
	if submatch == nil {
		return nil
	}
	version, err := semver.ParseTolerant(submatch[6])
	if err != nil {
		version = semver.Version{}
	}
	return &BinaryLinks{
		URL:          URL,
		Tag:          submatch[1],
		OS:           ToOs(submatch[2]),
		Arch:         ToArch(submatch[3]),
		Distributive: submatch[5],
		Version:      version,
	}
}

func MakeBinaryLinkByTag(Tag string) *BinaryLinks {
	submatch := TagRegexp.FindStringSubmatch(Tag)
	if submatch == nil {
		return nil
	}
	version, err := semver.ParseTolerant(submatch[5])
	if err != nil {
		version = semver.Version{}
	}
	return &BinaryLinks{
		URL:          "",
		Tag:          Tag,
		OS:           ToOs(submatch[1]),
		Arch:         ToArch(submatch[2]),
		Distributive: submatch[4],
		Version:      version,
	}
}

type OperationSystem string

var (
	OperationSystemUnspecified OperationSystem = ""
	OperationSystemMacos       OperationSystem = "macos"
	OperationSystemOsx         OperationSystem = "osx"
	OperationSystemOsxSsl      OperationSystem = "osx-ssl"
	OperationSystemLinux       OperationSystem = "linux"

	OperationSystems = []OperationSystem{
		OperationSystemMacos,
		OperationSystemOsx,
		OperationSystemOsxSsl,
		OperationSystemLinux,
	}
)

func ToOs(os string) OperationSystem {
	candidate := OperationSystem(os)
	switch candidate {
	case OperationSystemMacos:
		fallthrough
	case OperationSystemOsx:
		fallthrough
	case OperationSystemOsxSsl:
		fallthrough
	case OperationSystemLinux:
		return candidate
	}
	return OperationSystemUnspecified
}

type Arch string

var (
	ArchUnspecified Arch = ""
	ArchAMD64       Arch = "x86_64"
	ArchARM64       Arch = "arm64"
	ArchAArch64     Arch = "aarch64"

	Archs = []Arch{
		ArchAMD64,
		ArchARM64,
		ArchAArch64,
	}
)

func ToArch(arch string) Arch {
	candidate := Arch(arch)
	switch candidate {
	case ArchAMD64:
		fallthrough
	case ArchARM64:
		fallthrough
	case ArchAArch64:
		return candidate
	}
	return ArchUnspecified
}

// TakeMaxPatchVersion method is useful when you wish to find
// the most modern patch
func TakeMaxPatchVersion(links []BinaryLinks) ([]BinaryLinks, error) {
	type versionKey struct {
		Arch         Arch
		Distributive string
		Major        uint64
		Minor        uint64
	}
	resultMap := map[versionKey]BinaryLinks{}
	for _, link := range links {
		key := versionKey{
			Arch:         link.Arch,
			Distributive: link.Distributive,
			Major:        link.Version.Major,
			Minor:        link.Version.Minor,
		}
		oldLink, ok := resultMap[key]
		if !ok {
			resultMap[key] = link
			continue
		}

		if oldLink.Version.LT(link.Version) {
			resultMap[key] = link
			continue
		}
	}

	result := []BinaryLinks{}
	for _, link := range resultMap {
		result = append(result, link)
	}
	return result, nil
}

// TakeMaxDistrVersion method is useful when you wish to find
// the most modern distribution (e.g. ubuntu1804 vs ubuntu2004)
func TakeMaxDistrVersion(links []BinaryLinks) ([]BinaryLinks, error) {
	type versionKey struct {
		Arch    Arch
		Version string
	}
	resultMap := map[versionKey]BinaryLinks{}
	for _, link := range links {
		key := versionKey{
			Arch:    link.Arch,
			Version: link.Version.String(),
		}
		oldLink, ok := resultMap[key]
		if !ok {
			resultMap[key] = link
			continue
		}

		if oldLink.Distributive < link.Distributive {
			resultMap[key] = link
			continue
		}
	}

	result := []BinaryLinks{}
	for _, link := range resultMap {
		result = append(result, link)
	}
	return result, nil
}
