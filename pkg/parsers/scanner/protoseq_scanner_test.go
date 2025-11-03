package scanner

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

type protoseqEncoder struct {
	sync []byte
	res  bytes.Buffer
}

func newprotoseqEncoder(t *testing.T) *protoseqEncoder {
	seq := "1FF7F77EBEA65E9E37A6F62EFEAE47A7B76EBFAF169E9F37F657F766A706AFF7"
	hexSeq, err := hex.DecodeString(seq)
	require.NoError(t, err)

	return &protoseqEncoder{
		sync: hexSeq,
	}
}

func (e *protoseqEncoder) WriteSize(size int) {
	var prefix [4]byte
	binary.LittleEndian.PutUint32(prefix[:], uint32(size))
	e.res.Write(prefix[:])
}

func (e *protoseqEncoder) WriteData(data []byte) {
	e.res.Write(data)
}

func (e *protoseqEncoder) Result() []byte {
	return e.res.Bytes()
}

func TestProtoseqScanOK(t *testing.T) {
	exp := [][]byte{
		[]byte("123"),
		[]byte("\n\n\n\t\t\tr\r\r\r\r2xvzxc3"),
		[]byte("jbqwedvkfsln'mov[c"),
	}

	enc := newprotoseqEncoder(t)
	for _, item := range exp {
		enc.WriteSize(len(item))
		enc.WriteData(item)
		enc.WriteData(enc.sync)
	}

	scanner := NewProtoseqScanner(enc.Result())

	var got [][]byte
	for scanner.Scan() {
		data, err := scanner.Event()
		require.NoError(t, err)

		got = append(got, data)
	}

	require.NoError(t, scanner.Err())
	require.Equal(t, exp, got)
}

func TestProtoseqScanErrScan(t *testing.T) {
	// to shirt
	enc := newprotoseqEncoder(t)
	enc.WriteData([]byte("12"))

	scanner := NewProtoseqScanner(enc.Result())
	require.True(t, scanner.Scan())
	_, err := scanner.Event()
	require.Error(t, err)
	require.Error(t, scanner.Err())
	require.False(t, scanner.Scan())

	// not full
	enc = newprotoseqEncoder(t)
	enc.WriteSize(100)
	enc.WriteData([]byte("12"))

	scanner = NewProtoseqScanner(enc.Result())
	require.True(t, scanner.Scan())
	_, err = scanner.Event()
	require.Error(t, err)
	require.Error(t, scanner.Err())
	require.False(t, scanner.Scan())

	// not full sync
	enc = newprotoseqEncoder(t)
	data := []byte("lsjdflskjdlfkjsldk")
	enc.WriteSize(len(data))
	enc.WriteData(data)
	enc.WriteData(enc.sync[:30])

	scanner = NewProtoseqScanner(enc.Result())
	require.True(t, scanner.Scan())
	_, err = scanner.Event()
	require.Error(t, err)
	require.Error(t, scanner.Err())
	require.False(t, scanner.Scan())

	// no sync
	enc = newprotoseqEncoder(t)
	data = []byte("lsjdflskjdlfkjsldk")
	enc.WriteSize(len(data))
	enc.WriteData(data)

	scanner = NewProtoseqScanner(enc.Result())
	require.True(t, scanner.Scan())
	_, err = scanner.Event()
	require.Error(t, err)
	require.Error(t, scanner.Err())
	require.False(t, scanner.Scan())
}

func TestProtoseqScanEventCorrupted(t *testing.T) {
	// size less
	enc := newprotoseqEncoder(t)
	data := []byte("lsjdflskjdlfkjsldk")
	enc.WriteSize(len(data) - 1)
	enc.WriteData(data)
	enc.WriteData(enc.sync)

	scanner := NewProtoseqScanner(enc.Result())
	require.True(t, scanner.Scan())
	_, err := scanner.Event()
	require.Error(t, err)

	require.False(t, scanner.Scan())
	require.NoError(t, scanner.Err())

	// size more
	enc = newprotoseqEncoder(t)
	data = []byte("lsjdflskjdlfkjsldk")
	enc.WriteSize(len(data) - 1)
	enc.WriteData(data)
	enc.WriteData(enc.sync)

	scanner = NewProtoseqScanner(enc.Result())
	require.True(t, scanner.Scan())
	_, err = scanner.Event()
	require.Error(t, err)

	require.False(t, scanner.Scan())
	require.NoError(t, scanner.Err())

	// no size
	enc = newprotoseqEncoder(t)
	data = []byte("lsjdflskjdlfkjsldk")
	enc.WriteData(data)
	enc.WriteData(enc.sync)

	scanner = NewProtoseqScanner(enc.Result())
	require.True(t, scanner.Scan())
	_, err = scanner.Event()
	require.Error(t, err)

	require.False(t, scanner.Scan())
	require.NoError(t, scanner.Err())

	// first corrupted, second OK
	enc = newprotoseqEncoder(t)
	data = []byte("lsjdflskjdlfkjsldk")
	enc.WriteData(data)
	enc.WriteData(enc.sync)

	enc.WriteSize(len(data))
	enc.WriteData(data)
	enc.WriteData(enc.sync)

	scanner = NewProtoseqScanner(enc.Result())
	require.True(t, scanner.Scan())
	_, err = scanner.Event()
	require.Error(t, err)

	require.True(t, scanner.Scan())
	got, err := scanner.Event()
	require.NoError(t, err)
	require.Equal(t, data, got)

	require.False(t, scanner.Scan())
	require.NoError(t, scanner.Err())
}
