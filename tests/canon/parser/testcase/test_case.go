package testcase

import (
	"embed"
	"encoding/json"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/parsers"
	_ "github.com/transferia/transferia/pkg/parsers/registry"
	"github.com/transferia/transferia/pkg/providers/kafka"
	"go.ytsaurus.tech/library/go/core/log"
)

type TestCase struct {
	TopicName    string
	ParserConfig parsers.AbstractParserConfig
	Data         parsers.Message
}

func LoadStaticTestCases(t *testing.T, samples embed.FS) map[string]TestCase {
	cases := map[string]TestCase{}
	require.NoError(t, fs.WalkDir(samples, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d == nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".config.json") {
			return nil
		}
		caseName := strings.ReplaceAll(filepath.Base(path), ".config.json", "")
		configData, err := fs.ReadFile(samples, path)
		if err != nil {
			return err
		}
		sampleData, err := fs.ReadFile(samples, filepath.Dir(path)+"/"+caseName+".sample")
		if err != nil {
			return err
		}
		var source kafka.KafkaSource
		if err := json.Unmarshal(configData, &source); err != nil {
			logger.Log.Warn("unable to unmarshal", log.Error(err))
			return nil
		}
		source.WithDefaults()
		parserConfig, err := parsers.ParserConfigMapToStruct(source.ParserConfig)
		require.NoError(t, err)

		cases[caseName] = TestCase{
			TopicName:    source.GroupTopics[0],
			ParserConfig: parserConfig,
			Data:         MakeDefaultPersqueueReadMessage(sampleData),
		}
		return nil
	}))
	return cases
}

func MakeDefaultPersqueueReadMessage(data []byte) parsers.Message {
	return parsers.Message{
		Offset:     123,
		SeqNo:      32,
		Key:        []byte("test_source_id"),
		CreateTime: time.Date(2020, 2, 2, 10, 2, 21, 0, time.UTC),
		WriteTime:  time.Date(2020, 2, 2, 10, 2, 20, 0, time.UTC),
		Value:      data,
		Headers:    map[string]string{"some_field": "test"},
	}
}
