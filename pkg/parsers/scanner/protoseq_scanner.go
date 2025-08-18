package scanner

import (
	"bytes"
	"encoding/binary"
	"io"

	"golang.org/x/exp/slices"
	"golang.org/x/xerrors"
)

var (
	// magic protoseq sequence: 1FF7F77EBEA65E9E37A6F62EFEAE47A7B76EBFAF169E9F37F657F766A706AFF7
	magicSeq = []byte{
		31, 247, 247, 126, 190, 166, 94, 158, 55, 166, 246, 46, 254,
		174, 71, 167, 183, 110, 191, 175, 22, 158, 159, 55, 246, 87,
		247, 102, 167, 6, 175, 247,
	}

	// 64mb - const from push-client
	maxRecordSize uint32 = 64 * 1024 * 1024
)

type ProtoseqScanner struct {
	frameSyncrodata []byte
	data            []byte
	maxRecordSize   uint32
	scanError       error
	lastEvent       *Event
}

func NewProtoseqScanner(data []byte) *ProtoseqScanner {
	return &ProtoseqScanner{
		data:            data,
		frameSyncrodata: magicSeq,
		maxRecordSize:   maxRecordSize,
		scanError:       nil,
		lastEvent:       nil,
	}
}

func (s *ProtoseqScanner) Scan() bool {
	if s.scanError != nil {
		return false
	}

	if len(s.data) == 0 {
		s.scanError = io.EOF
		return false
	}

	if len(s.data) < 4 {
		s.lastEvent = &Event{
			data: s.data,
			err:  xerrors.Errorf("last incomplete protoseq message: expected size len = 4, got size len = %d", len(s.data)),
		}
		s.data = nil
		return true
	}

	size := binary.LittleEndian.Uint32(s.data[:4])
	s.data = s.data[4:]

	if size > s.maxRecordSize {
		return s.handleCurrentCorrupted()
	}

	frameLen := int(size) + len(s.frameSyncrodata)
	if len(s.data) < frameLen {
		eventData := make([]byte, 4+len(s.data))
		binary.LittleEndian.PutUint32(eventData[:4], size)
		copy(eventData[4:], s.data)
		s.lastEvent = &Event{
			data: eventData,
			err:  xerrors.New("last incomplete protoseq message"),
		}
		s.data = nil
		return true
	}

	if !slices.Equal(s.data[size:frameLen], s.frameSyncrodata) {
		return s.handleCurrentCorrupted()
	}

	s.lastEvent = &Event{
		data: s.data[:size],
		err:  nil,
	}
	s.data = s.data[frameLen:]

	return true
}

func (s *ProtoseqScanner) handleCurrentCorrupted() bool {
	syncDataID := bytes.Index(s.data, s.frameSyncrodata)
	if syncDataID < 0 {
		s.scanError = xerrors.New("can't find any magic sequence entry")
		return false
	}

	s.lastEvent = &Event{
		data: s.data[:syncDataID],
		err:  xerrors.New("frame corrupted"),
	}
	s.data = s.data[syncDataID+len(s.frameSyncrodata):]

	return true
}

func (s *ProtoseqScanner) Event() ([]byte, error) {
	if s.lastEvent == nil {
		return nil, xerrors.New("event was not initialized")
	}

	return s.lastEvent.data, s.lastEvent.err
}

func (s *ProtoseqScanner) Err() error {
	if s.scanError == io.EOF {
		return nil
	}

	if s.lastEvent != nil && s.lastEvent.err != nil {
		return xerrors.Errorf("last event error: %w", s.lastEvent.err)
	}

	return s.scanError
}
