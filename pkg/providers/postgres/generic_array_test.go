//go:build !disable_postgres_provider

package postgres

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
)

func TestGenericArray_DecodeText(t *testing.T) {
	t.Run("uuid array", func(t *testing.T) {
		ga := new(GenericArray)
		ga.newElement = func() (pgtype.Value, error) {
			return new(pgtype.UUID), nil
		}
		uuidArray := pgtype.UUIDArray{
			Elements: []pgtype.UUID{
				{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
				{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		}
		text, err := uuidArray.EncodeText(new(pgtype.ConnInfo), nil)
		require.NoError(t, err)
		require.Equal(t, "{00010203-0405-0607-0809-0a0b0c0d0e0f,NULL}", string(text))
		err = ga.DecodeText(new(pgtype.ConnInfo), []byte("{00010203-0405-0607-0809-0a0b0c0d0e0f,NULL}"))
		require.NoError(t, err)
		val, err := ga.ExtractValue(new(pgtype.ConnInfo))
		require.NoError(t, err)
		require.Len(t, val, 2)
		require.Equal(t, []interface{}{"00010203-0405-0607-0809-0a0b0c0d0e0f", "00000000-0000-0000-0000-000000000000"}, val)
	})
	t.Run("simple arr", func(t *testing.T) {
		ga := new(GenericArray)
		ga.newElement = func() (pgtype.Value, error) {
			return new(pgtype.Int4), nil
		}
		err := ga.DecodeText(new(pgtype.ConnInfo), []byte("{1,2,3}"))
		require.NoError(t, err)
		val, err := ga.ExtractValue(new(pgtype.ConnInfo))
		require.NoError(t, err)
		require.Len(t, val, 3)
		require.Equal(t, []interface{}{int32(1), int32(2), int32(3)}, val)
	})
	t.Run("matrix", func(t *testing.T) {
		ga := new(GenericArray)
		ga.newElement = func() (pgtype.Value, error) {
			return new(pgtype.Int4), nil
		}
		err := ga.DecodeText(new(pgtype.ConnInfo), []byte("{{1,2,3},{4,5,6}}"))
		require.NoError(t, err)
		val, err := ga.ExtractValue(new(pgtype.ConnInfo))
		require.NoError(t, err)
		require.Len(t, val, 2)
		require.Equal(t, []interface{}{[]interface{}{int32(1), int32(2), int32(3)}, []interface{}{int32(4), int32(5), int32(6)}}, val)
	})
	t.Run("mega matrix 1", func(t *testing.T) {
		ga := new(GenericArray)
		ga.logger = logger.Log
		ga.newElement = func() (pgtype.Value, error) {
			return new(pgtype.Int4), nil
		}
		err := ga.DecodeText(new(pgtype.ConnInfo), []byte("[1:1][2:3][3:5]={{{1,2,3},{4,5,6}}}"))
		require.NoError(t, err)
		val, err := ga.ExtractValue(new(pgtype.ConnInfo))
		require.NoError(t, err)
		require.Len(t, val, 1)
		require.Equal(t, []interface{}{[]interface{}{[]interface{}{int32(1), int32(2), int32(3)}, []interface{}{int32(4), int32(5), int32(6)}}}, val)
	})
	t.Run("mega matrix 2", func(t *testing.T) {
		ga := new(GenericArray)
		ga.logger = logger.Log
		ga.newElement = func() (pgtype.Value, error) {
			return new(pgtype.Int4), nil
		}
		err := ga.DecodeText(new(pgtype.ConnInfo), []byte("[0:0][0:1][0:2]={{{1,2,3},{4,5,6}}}"))
		require.NoError(t, err)
		val, err := ga.ExtractValue(new(pgtype.ConnInfo))
		require.NoError(t, err)
		require.Len(t, val, 1)
		require.Equal(t, []interface{}{[]interface{}{[]interface{}{int32(1), int32(2), int32(3)}, []interface{}{int32(4), int32(5), int32(6)}}}, val)
	})
	t.Run("mega matrix 3", func(t *testing.T) {
		ga := new(GenericArray)
		ga.logger = logger.Log
		ga.newElement = func() (pgtype.Value, error) {
			return new(pgtype.Int4), nil
		}
		err := ga.DecodeText(new(pgtype.ConnInfo), []byte("[-x:-1][0:1][0:2]={{{1,2,3},{4,5,6}}}"))
		require.Error(t, err)
		val, err := ga.ExtractValue(new(pgtype.ConnInfo))
		require.NoError(t, err)
		require.Equal(t, "[-x:-1][0:1][0:2]={{{1,2,3},{4,5,6}}}", val)
	})
}
