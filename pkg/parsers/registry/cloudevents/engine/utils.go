package engine

import (
	"log"
	"net/url"
	"regexp"
	"strconv"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

var extractSubjectAndVersion = regexp.MustCompile(`.*/schemas/ids/(.*)`)

func extractSchemaIDAndURL(in string) (hostPort string, schemaID uint32, err error) {
	u, err := url.Parse(in)
	if err != nil {
		log.Fatal(err)
	}

	hostPort = u.Scheme + "://" + u.Hostname()
	port := u.Port()
	if port != "" {
		hostPort += ":" + port
	}

	arr := extractSubjectAndVersion.FindStringSubmatch(in)
	if len(arr) != 2 {
		return "", 0, xerrors.Errorf("unable to find subject & version into URL, url:%s", in)
	}

	schemaIDInt, err := strconv.Atoi(arr[1])
	if err != nil {
		return "", 0, xerrors.Errorf("unable to convert version string to int, url:%s", in)
	}

	return hostPort, uint32(schemaIDInt), nil
}
