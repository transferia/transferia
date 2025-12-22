package postgres

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/library/go/core/log"
)

type Wal2JsonParser struct {
	decoder      *json.Decoder
	err          error
	mutex        sync.Mutex
	outCh        chan *Wal2JSONItem
	reader       *util.ChannelReader
	readerClosed bool
}

func (p *Wal2JsonParser) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.reader.Close()
	for range p.outCh {
	}
	p.readerClosed = true
}

func (p *Wal2JsonParser) Parse(data []byte) (result []*Wal2JSONItem, err error) {
	if p.err != nil {
		//nolint:descriptiveerrors
		return nil, p.err
	}

	var emptyBuf []byte

	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.readerClosed {
		return nil, xerrors.New("Attempt to write to closed channel at Wal2JsonParser.Parse")
	}

	p.reader.Input() <- data
	for {
		select {
		case item, ok := <-p.outCh:
			if !ok {
				//nolint:descriptiveerrors
				return result, p.err
			}
			result = append(result, item)
		case p.reader.Input() <- emptyBuf:
			// JSON parser needs more input data, return the result parsed so far
			return result, nil
		}
	}
}

func (p *Wal2JsonParser) readDelim(expectedDelim rune) error {
	token, err := p.decoder.Token()
	if err != nil {
		return xerrors.Errorf("Cannot read JSON token: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || rune(delim) != expectedDelim {
		return xerrors.Errorf("Expected '%c' but got '%c'", expectedDelim, delim)
	}
	return nil
}

func (p *Wal2JsonParser) readKey() (string, error) {
	token, err := p.decoder.Token()
	if err != nil {
		return "", xerrors.Errorf("Cannot read JSON token: %w", err)
	}
	key, ok := token.(string)
	if !ok {
		return "", xerrors.Errorf("Expected string key but got %v", token)
	}
	return key, nil
}

func (p *Wal2JsonParser) readXID() (uint32, error) {
	token, err := p.decoder.Token()
	if err != nil {
		return 0, xerrors.Errorf("Cannot read JSON token: %w", err)
	}
	num, ok := token.(json.Number)
	if !ok {
		return 0, xerrors.Errorf("Expected JSON number but got %v", token)
	}
	xid, err := num.Int64()
	if err != nil {
		return 0, xerrors.Errorf("Cannot parse JSON number: %w", err)
	}
	return uint32(xid), nil
}

func extractTimestamp(raw string) (uint64, error) {
	template := "2006-01-02 15:04:05.999999999-07"
	t, err := time.Parse(template, raw)
	if err != nil {
		return 0, err
	}
	return uint64(t.UnixNano()), nil
}

func (p *Wal2JsonParser) readTimestamp() (uint64, error) {
	token, err := p.decoder.Token()
	if err != nil {
		return 0, xerrors.Errorf("Cannot read JSON token: %w", err)
	}
	stringTimestamp, ok := token.(string)
	if !ok {
		return 0, xerrors.Errorf("Expected JSON string but got %v", token)
	}
	timestamp, err := extractTimestamp(stringTimestamp)
	if err != nil {
		return 0, err
	}
	return timestamp, nil
}

func fillToastValuesFromOldKeys(item *Wal2JSONItem) {
	if len(item.OldKeys.KeyNames) == 0 {
		return
	}

	// we need to check to avoid panic when filling toast values from old keys
	if len(item.OldKeys.KeyTypeOids) != len(item.OldKeys.KeyNames) ||
		len(item.OldKeys.KeyValues) != len(item.OldKeys.KeyNames) {
		logger.Log.Warn("mismatched old key values or type oids",
			log.String("table", item.Table),
			log.String("oldKeys.KeyNames", fmt.Sprintf("%v", item.OldKeys.KeyNames)),
			log.String("oldKeys.KeyTypeOids", fmt.Sprintf("%v", item.OldKeys.KeyTypeOids)),
			log.String("length of oldKeys.KeyValues", fmt.Sprintf("%v", len(item.OldKeys.KeyValues))),
			log.String("length of oldKeys.KeyTypeOids", fmt.Sprintf("%v", len(item.OldKeys.KeyTypeOids))),
			log.String("length of oldKeys.KeyNames", fmt.Sprintf("%v", len(item.OldKeys.KeyNames))),
		)
		// if mismatched, we don't fill toast values
		return
	}

	existingColumns := make(map[string]bool, len(item.ColumnNames))
	for _, colName := range item.ColumnNames {
		existingColumns[colName] = true
	}

	for i, oldKeyName := range item.OldKeys.KeyNames {
		if !existingColumns[oldKeyName] {
			logger.Log.Debug("filling toast values from old keys",
				log.String("table", item.Table),
				log.String("added toast value column name", oldKeyName),
				log.String("columnNames", fmt.Sprintf("%v", item.ColumnNames)),
				log.String("oldKeys.KeyNames", fmt.Sprintf("%v", item.OldKeys.KeyNames)),
				log.String("columnTypeOIDs", fmt.Sprintf("%v", item.ColumnTypeOIDs)),
				log.String("oldKeys.KeyTypeOids", fmt.Sprintf("%v", item.OldKeys.KeyTypeOids)),
			)
			item.ColumnNames = append(item.ColumnNames, oldKeyName)
			item.ColumnValues = append(item.ColumnValues, item.OldKeys.KeyValues[i])
			item.ColumnTypeOIDs = append(item.ColumnTypeOIDs, item.OldKeys.KeyTypeOids[i])
		}
	}
}

func (p *Wal2JsonParser) parseLoop() {
	defer close(p.outCh)

	for {
		if err := p.readDelim('{'); err != nil {
			p.err = xerrors.Errorf("Cannot parse changeset object beginning: %w", err)
			return
		}

		var id uint32
		var timestamp uint64
		for p.decoder.More() {
			key, err := p.readKey()
			if err != nil {
				p.err = xerrors.Errorf("Cannot parse changeset object key: %w", err)
				return
			}

			switch key {
			case "xid":
				id, err = p.readXID()
				if err != nil {
					p.err = xerrors.Errorf("Cannot parse XID: %w", err)
					return
				}
			case "timestamp":
				timestamp, err = p.readTimestamp()
				if err != nil {
					p.err = xerrors.Errorf("Cannot parse changeset timestamp: %w", err)
					return
				}
			case "change":
				if err := p.readDelim('['); err != nil {
					p.err = xerrors.Errorf("Cannot parse change array beginning: %w", err)
					return
				}
				for p.decoder.More() {
					item := new(Wal2JSONItem)
					decoderPositionBefore := p.decoder.InputOffset()
					if err := p.decoder.Decode(item); err != nil {
						p.err = xerrors.Errorf("Cannot decode change item: %w", err)
						return
					}
					readRawBytes := uint64(p.decoder.InputOffset() - decoderPositionBefore)
					item.ID = id
					item.CommitTime = timestamp

					// for toast values to avoid missing values in columnValues, we fill them from oldKeys
					if item.Kind == abstract.UpdateKind {
						fillToastValuesFromOldKeys(item)
					}

					item.Size.Read = readRawBytes
					p.outCh <- item
				}
				if err := p.readDelim(']'); err != nil {
					p.err = xerrors.Errorf("Cannot parse change array end: %w", err)
					return
				}
			default:
				var scratch interface{}
				if err := p.decoder.Decode(&scratch); err != nil {
					p.err = xerrors.Errorf("Cannot decode changeset field at key %s: %w", key, err)
					return
				}
			}
		}
		if err := p.readDelim('}'); err != nil {
			p.err = xerrors.Errorf("Cannot parse changeset object end: %w", err)
			return
		}
	}
}

func NewWal2JsonParser() *Wal2JsonParser {
	reader := util.NewChannelReader()
	decoder := jsonx.NewDefaultDecoder(reader)
	p := &Wal2JsonParser{
		outCh:        make(chan *Wal2JSONItem),
		reader:       reader,
		decoder:      decoder,
		err:          nil,
		mutex:        sync.Mutex{},
		readerClosed: false,
	}
	go p.parseLoop()
	return p
}
