package canon

import (
	"embed"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/ydb"
	ytprovider "github.com/transferia/transferia/pkg/providers/yt"
	"golang.org/x/exp/slices"
)

var (
	//go:embed postgres/gotest/canondata/*/extracted
	PostgresCanon embed.FS
	//go:embed clickhouse/canondata/*
	ClickhouseCanon embed.FS
	//go:embed mysql/canondata/*/extracted
	MysqlCanon embed.FS
	//go:embed ydb/canondata/*/extracted
	YdbCanon embed.FS
	//go:embed yt/canondata/*/extracted
	YtCanon embed.FS
)

func init() {
	gob.Register(new(time.Time))
	gob.Register(new(time.Duration))
	gob.Register(new(json.Number))
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
}

var (
	AllCanon = map[abstract.ProviderType]embed.FS{
		postgres.ProviderType:   PostgresCanon,
		mysql.ProviderType:      MysqlCanon,
		clickhouse.ProviderType: ClickhouseCanon,
		ytprovider.ProviderType: YtCanon,
		ydb.ProviderType:        YdbCanon,
	}
	Roots = map[abstract.ProviderType]string{
		postgres.ProviderType:   "postgres",
		clickhouse.ProviderType: "clickhouse",
	}
)

type CanonizedCase struct {
	Name string
	Type abstract.ProviderType
	Data []abstract.ChangeItem
}

func (c CanonizedCase) String() string {
	return fmt.Sprintf("%s::%s", c.Type, c.Name)
}

func All(sources ...abstract.ProviderType) []CanonizedCase {
	var res []CanonizedCase

	for _, source := range sources {
		cases, ok := AllCanon[source]
		if !ok {
			continue
		}

		root := string(source)
		if altRoot, ok := Roots[source]; ok {
			root = altRoot
		}
		if err := fs.WalkDir(cases, root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d == nil {
				return nil
			}
			if d.IsDir() {
				return nil
			}
			data, err := fs.ReadFile(cases, path)
			if err != nil {
				return err
			}
			if filepath.Base(path) != "extracted" {
				return nil
			}
			parts := strings.Split(filepath.Base(filepath.Dir(path)), ".")
			name := parts[len(parts)-1]
			var typedRows []abstract.TypedChangeItem
			if err := json.Unmarshal(data, &typedRows); err != nil {
				logger.Log.Errorf("unable to parse test case %s: %v", path, err)
			}
			var rows []abstract.ChangeItem
			for _, typedRow := range typedRows {
				rows = append(rows, abstract.ChangeItem(typedRow))
			}
			res = append(res, CanonizedCase{
				Name: name,
				Type: source,
				Data: rows,
			})
			return nil
		}); err != nil {
			logger.Log.Fatalf("unable to walk all extracted stuff: %v", err)
		}
	}
	slices.SortFunc(res, func(a, b CanonizedCase) int {
		return -strings.Compare(a.Name, b.Name)
	})
	return res
}
