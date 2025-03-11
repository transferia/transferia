package warmup

import (
	"bytes"
	"encoding/binary"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

// It's important to warn-up Schema-Registry cache single-thread, to not to DDoS Schema-Registry
// mutex we need not bcs of something is thread-unsafe, but to reduce schema-registry RPS
func WarmUpSRCache(logger log.Logger, mutex *sync.Mutex, batch parsers.MessageBatch, schemaRegistryClient *confluent.SchemaRegistryClient, notFoundIsOk bool) {
	extractSchemaID := func(buf []byte) (uint32, []byte) {
		msgLen := len(buf)
		if len(buf) != 0 {
			if buf[0] == 0 {
				zeroIndex := bytes.Index(buf[5:], []byte{0})
				if zeroIndex != -1 {
					msgLen = 5 + zeroIndex
				}
			}
		}
		return binary.BigEndian.Uint32(buf[1:5]), buf[msgLen:]
	}

	schemaIDs := set.New[uint32]()
	var schemaID uint32 = 0
	for _, currMsg := range batch.Messages {
		leastBuf := currMsg.Value
		for {
			if len(leastBuf) == 0 {
				break
			}
			schemaID, leastBuf = extractSchemaID(leastBuf)
			schemaIDs.Add(schemaID)
		}
	}

	mutex.Lock()
	defer mutex.Unlock()
	for _, currSchemaID := range schemaIDs.Slice() {
		_ = backoff.RetryNotify(func() error {
			_, err := schemaRegistryClient.GetSchema(int(currSchemaID))
			if notFoundIsOk && err != nil && strings.Contains(err.Error(), "Error code: 404") {
				return nil
			}
			return err
		}, backoff.NewConstantBackOff(time.Second), util.BackoffLogger(logger, "getting schema (warm-up cache)"))
	}
}
