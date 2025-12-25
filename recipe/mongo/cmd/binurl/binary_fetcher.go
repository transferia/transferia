package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/util/cli"
	"github.com/transferia/transferia/recipe/mongo/pkg/binurl"
)

var (
	osFlag               string
	archFlag             string
	distributiveSubstr   string
	minDistroFilter      string
	maxDistroFilter      string
	patchCollapseFlag    bool
	maxDistrCollapseFlag bool
)

type CmdOS string

var (
	CmdOSUnspecified CmdOS = ""
	CmdOSMacos       CmdOS = "macos"
	CmdOSLinux       CmdOS = "linux"
)

type CmdArch string

var (
	CmdArchUnspecified CmdArch = ""
	CmdArchAMD64       CmdArch = "x86_64"
	CmdArchARM64       CmdArch = "arm64"
)

func main() {
	flag.StringVar(&osFlag, "os", "", "specify OS, available values: macos | linux")
	flag.StringVar(&distributiveSubstr, "tag-contains", "", "check that tag has contain specified string (useful when finding certain distributives)")
	flag.StringVar(&archFlag, "arch", "", "specify arch, available values: x86_64 | arm64")
	flag.BoolVar(&patchCollapseFlag, "patch-collapse", false, "specify this flag to leave only major patches per each version")
	flag.StringVar(&minDistroFilter, "min-distro", "", "filters distributions to be greater or equal than specified value, e.g. 'ubuntu1804' specifies version greater or equal than 18.04 version of Ubuntu")
	flag.StringVar(&maxDistroFilter, "max-distro", "", "filters distributions to be less or equal than specified value, e.g. 'ubuntu1804' specifies version less or equal than 18.04 version of Ubuntu")
	flag.BoolVar(&maxDistrCollapseFlag, "max-distr-collapse", false, "when distribution of OS differs like 'ubuntu1804...' and 'ubuntu2004', you may want to take freshest one")
	flag.BoolVar(&maxDistrCollapseFlag, "no-sleep", false, "do not sleep before merging files into single archive")
	flag.Parse()

	cmdOS := CmdOS(osFlag)
	switch cmdOS {
	case CmdOSUnspecified:
	case CmdOSMacos:
	case CmdOSLinux:
	default:
		fmt.Println("invalid value for flag --os")
		flag.PrintDefaults()
		os.Exit(2)
	}

	arch := CmdArch(archFlag)
	switch arch {
	case CmdArchUnspecified:
	case CmdArchAMD64:
	case CmdArchARM64:
	default:
		fmt.Println("invalid value for flag --arch")
		flag.PrintDefaults()
		os.Exit(3)
	}

	var links []binurl.BinaryLinks
	// make fancy CLI while waiting for heavy operation
	sp := cli.NewSpinner(7, 70*time.Millisecond)
	sp.Start()

	//links, err := FetchAllBinaries()
	var err error
	links, err = getLinksByCriteria(
		cmdOS,
		arch,
		distributiveSubstr,
		minDistroFilter,
		maxDistroFilter,
		patchCollapseFlag,
		maxDistrCollapseFlag,
	)
	if err != nil {
		fmt.Println("Cannot get links by criteria:", err)
		os.Exit(4)
	}

	sp.Stop()

	fmt.Printf("Fetched %d links\n", len(links))

	printOutList(links)
}

func printOutList(links []binurl.BinaryLinks) {
	for _, link := range links {
		fmt.Printf("%7s(%7s), %11s -> %s\n", link.OS, link.Arch, link.Version, link.URL)
	}
}

func getLinksByCriteria(
	operationSystem CmdOS,
	arch CmdArch,
	distributiveSubstr string,
	minDistributive string,
	maxDistributive string,
	patchCollapse bool,
	maxDistrCollapse bool,
) ([]binurl.BinaryLinks, error) {
	links, err := FetchAllBinaries()
	if err != nil {
		return nil, xerrors.Errorf("cannot fetch all binaries: %w", err)
	}

	switch operationSystem {
	case CmdOSUnspecified:
	case CmdOSMacos:
		links = yslices.Filter(links, func(link binurl.BinaryLinks) bool {
			switch link.OS {
			case binurl.OperationSystemMacos:
				return true
			case binurl.OperationSystemOsx:
				return true
			case binurl.OperationSystemOsxSsl:
				return true
			}
			return false
		})
	case CmdOSLinux:
		links = yslices.Filter(links, func(link binurl.BinaryLinks) bool {
			switch link.OS {
			case binurl.OperationSystemLinux:
				return true
			}
			return false
		})
	}

	switch arch {
	case CmdArchUnspecified:
	case CmdArchAMD64:
		links = yslices.Filter(links, func(link binurl.BinaryLinks) bool {
			return link.Arch == binurl.ArchAMD64
		})
	case CmdArchARM64:
		links = yslices.Filter(links, func(link binurl.BinaryLinks) bool {
			return link.Arch == binurl.ArchARM64 || link.Arch == binurl.ArchAArch64
		})

	}

	links = yslices.Filter(links, func(link binurl.BinaryLinks) bool {
		tag := strings.TrimSpace(link.Tag)
		distro := strings.TrimSpace(distributiveSubstr)
		return strings.Contains(tag, distro) &&
			(maxDistributive == "" || link.Distributive <= maxDistributive) &&
			(minDistributive == "" || link.Distributive >= minDistributive)
	})

	if patchCollapse {
		links, err = binurl.TakeMaxPatchVersion(links)
		if err != nil {
			return nil, xerrors.Errorf("cannot aggregate out patch versions: %w", err)
		}
	}
	if maxDistrCollapse {
		links, err = binurl.TakeMaxDistrVersion(links)
		if err != nil {
			return nil, xerrors.Errorf("cannot aggregate out max distributions versions: %w", err)
		}
	}
	return links, nil
}

func fetchMongoDBArchivePage() ([]byte, error) {
	pageURL := binurl.MongodbArchiveURL
	rsp, err := http.Get(pageURL)
	if err != nil {
		return nil, xerrors.Errorf("unable to get page '%s': %w", pageURL, err)
	}
	if rsp.StatusCode/100 != 2 {
		return nil, xerrors.Errorf("status code is not 2xx, actually it is %d", rsp.StatusCode)
	}
	defer rsp.Body.Close()

	return io.ReadAll(rsp.Body)
}

func FetchAllBinaries() ([]binurl.BinaryLinks, error) {
	bytes, err := fetchMongoDBArchivePage()
	if err != nil {
		return nil, xerrors.Errorf("cannot fetch mongodb archive page: %w", err)
	}
	htmlATags := binurl.HTMLARegexp.FindAllString(string(bytes), -1)

	links := []binurl.BinaryLinks{}
	for _, htmlATag := range htmlATags {
		link := binurl.MakeBinaryLinkByHTMLA(htmlATag)
		if link == nil {
			continue
		}
		links = append(links, *link)
	}
	return links, nil
}
