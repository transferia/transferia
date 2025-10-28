package reference

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/serializer"
	e2e "github.com/transferia/transferia/tests/canon"
	"golang.org/x/exp/slices"
)

func TestSerialize(t *testing.T) {
	tests := []struct {
		Name       string
		Serializer serializer.Serializer
	}{
		{
			Name:       "csv:default",
			Serializer: serializer.NewCsvSerializer(),
		},
		{
			Name: "json:default",
			Serializer: serializer.NewJSONSerializer(
				&serializer.JSONSerializerConfig{},
			),
		},
		{
			Name: "json:newline",
			Serializer: serializer.NewJSONSerializer(
				&serializer.JSONSerializerConfig{
					AddClosingNewLine: true,
				},
			),
		},
		{
			Name: "raw:default",
			Serializer: serializer.NewRawSerializer(
				&serializer.RawSerializerConfig{},
			),
		},
		{
			Name: "raw:newline",
			Serializer: serializer.NewRawSerializer(
				&serializer.RawSerializerConfig{
					AddClosingNewLine: true,
				},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			item := abstract.MakeRawMessage(
				[]byte("stub"), "table", time.Time{}, "topic", 42, 42, []byte("data"),
			)

			data, err := test.Serializer.Serialize(&item)
			require.NoError(t, err)

			err = os.WriteFile("result", data, 0644)
			require.NoError(t, err)

			canon.SaveFile(t, "result", canon.WithLocal(true))
		})
	}
}

func TestBatchSerializer(t *testing.T) {
	tests := []struct {
		Name       string
		Serializer serializer.BatchSerializer
		Generator  func(int) []*abstract.ChangeItem
	}{
		{
			Name: "csv:default",
			Serializer: serializer.NewCsvBatchSerializer(
				&serializer.CsvBatchSerializerConfig{},
			),
			Generator: ReadChangeItems,
		},
		{
			Name: "json:default",
			Serializer: serializer.NewJSONBatchSerializer(
				&serializer.JSONBatchSerializerConfig{},
			),
			Generator: ReadChangeItems,
		},
		{
			Name: "json:newline",
			Serializer: serializer.NewJSONBatchSerializer(
				&serializer.JSONBatchSerializerConfig{
					SerializerConfig: &serializer.JSONSerializerConfig{
						AddClosingNewLine: true,
					},
				},
			),
			Generator: ReadChangeItems,
		},
		{
			Name: "raw:default",
			Serializer: serializer.NewRawBatchSerializer(
				&serializer.RawBatchSerializerConfig{},
			),
			Generator: MakeChangeItems,
		},
		{
			Name: "raw:newline",
			Serializer: serializer.NewRawBatchSerializer(
				&serializer.RawBatchSerializerConfig{
					SerializerConfig: &serializer.RawSerializerConfig{
						AddClosingNewLine: true,
					},
				},
			),
			Generator: MakeChangeItems,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			items := test.Generator(10)

			data, err := test.Serializer.Serialize(items)
			require.NoError(t, err)

			err = os.WriteFile("result", data, 0644)
			require.NoError(t, err)

			canon.SaveFile(t, "result", canon.WithLocal(true))
		})
	}
}

func TestStreamSerializer(t *testing.T) {
	tests := []struct {
		Name          string
		NewSerializer func(io.Writer) serializer.StreamSerializer
		Generator     func(int) []*abstract.ChangeItem
	}{
		{
			Name: "csv:default",
			NewSerializer: func(w io.Writer) serializer.StreamSerializer {
				return serializer.NewCsvStreamSerializer(w)
			},
			Generator: ReadChangeItems,
		},
		{
			Name: "json:default",
			NewSerializer: func(w io.Writer) serializer.StreamSerializer {
				return serializer.NewJSONStreamSerializer(
					w, &serializer.JSONSerializerConfig{},
				)
			},
			Generator: ReadChangeItems,
		},
		{
			Name: "json:newline",
			NewSerializer: func(w io.Writer) serializer.StreamSerializer {
				return serializer.NewJSONStreamSerializer(
					w, &serializer.JSONSerializerConfig{
						AddClosingNewLine: true,
					},
				)
			},
			Generator: ReadChangeItems,
		},
		{
			Name: "raw:default",
			NewSerializer: func(w io.Writer) serializer.StreamSerializer {
				return serializer.NewRawStreamSerializer(
					w, &serializer.RawSerializerConfig{},
				)
			},
			Generator: MakeChangeItems,
		},
		{
			Name: "raw:newline",
			NewSerializer: func(w io.Writer) serializer.StreamSerializer {
				return serializer.NewRawStreamSerializer(
					w, &serializer.RawSerializerConfig{
						AddClosingNewLine: true,
					},
				)
			},
			Generator: MakeChangeItems,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			items := test.Generator(10)

			var buf bytes.Buffer
			s := test.NewSerializer(&buf)

			var (
				half = items[:len(items)/2]
				tail = items[len(half):]
			)

			err := s.Serialize(half)
			require.NoError(t, err)

			err = s.Serialize(tail)
			require.NoError(t, err)

			err = s.Close()
			require.NoError(t, err)

			err = os.WriteFile("result", buf.Bytes(), 0644)
			require.NoError(t, err)

			canon.SaveFile(t, "result", canon.WithLocal(true))
		})
	}
}

func ReadChangeItems(n int) []*abstract.ChangeItem {
	types := make([]abstract.ProviderType, 0, len(e2e.AllCanon))
	for typ := range e2e.AllCanon {
		types = append(types, typ)
	}
	slices.Sort(types)

	all := e2e.All(
		types...,
	)

	out := make([]*abstract.ChangeItem, 0, n*len(all))
	for i := range all {
		data := all[i].Data
		if len(data) > n {
			data = data[:n]
		}

		for i := range data {
			out = append(out, &data[i])
		}
	}

	return out
}

func MakeChangeItems(n int) []*abstract.ChangeItem {
	out := make([]*abstract.ChangeItem, n)
	for i := range out {
		item := abstract.MakeRawMessage(
			[]byte("stub"),
			fmt.Sprintf("table%d", i),
			time.Time{},
			fmt.Sprintf("topic%d", i),
			i,
			int64(i),
			fmt.Appendf(nil, "data%d", i),
		)

		out[i] = &item
	}

	return out
}
