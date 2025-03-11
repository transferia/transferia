package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	serializer "github.com/transferria/transferria/pkg/serializer/queue"
	confluentsrmock "github.com/transferria/transferria/tests/helpers/confluent_schema_registry_mock"
)

func getChangeItems(fullFilename string, limit int) ([]abstract.ChangeItem, error) {
	file, err := os.Open(fullFilename)
	if err != nil {
		return nil, xerrors.Errorf("unable to open file, err: %w", err)
	}
	defer file.Close()

	var tableSchema *abstract.TableSchema = nil
	result := make([]abstract.ChangeItem, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		currString := scanner.Text()
		changeItem, err := abstract.UnmarshalChangeItem([]byte(currString))
		if err != nil {
			return nil, xerrors.Errorf("unable to open file, err: %w", err)
		}
		if changeItem.Kind != abstract.InsertKind {
			continue
		}
		if tableSchema == nil {
			tableSchema = changeItem.TableSchema
		}
		changeItem.SetTableSchema(tableSchema)
		result = append(result, *changeItem)

		if len(result)%10000 == 0 {
			fmt.Printf("    loading items: %d\n", len(result))
		}

		if limit != -1 {
			if len(result) > limit {
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, xerrors.Errorf("unable to open file, err: %w", err)
	}

	return result, nil
}

func try(changeItems []abstract.ChangeItem, threadsNum, chunkSize int) (map[abstract.TablePartID][]serializer.SerializedMessage, error) {
	sr := confluentsrmock.NewConfluentSRMock(nil, nil)
	defer sr.Close()

	debeziumSerializer, err := serializer.NewDebeziumSerializer(map[string]string{
		"value.converter":                     "io.confluent.connect.json.JsonSchemaConverter",
		"dt.add.original.type.info":           "true",
		"value.converter.schema.registry.url": sr.URL(),
	}, false, true, false, logger.Log)
	if err != nil {
		return nil, xerrors.Errorf("unable to build debezium serializer, err: %w", err)
	}
	result, err := debeziumSerializer.SerializeImpl(changeItems, threadsNum, chunkSize)
	if err != nil {
		return nil, xerrors.Errorf("unable to serialize, err: %w", err)
	}
	return result, nil
}

func calcValsSize(in map[abstract.TablePartID][]serializer.SerializedMessage) uint64 {
	var resultSize uint64 = 0
	for _, messages := range in {
		for _, msg := range messages {
			resultSize += uint64(len(msg.Value))
		}
	}
	return resultSize
}

func timerWrapper(changeItems []abstract.ChangeItem, threadsNum, chunkSize int) (time.Duration, error) {
	runtime.GC()
	startTime := time.Now()
	result, err := try(changeItems, threadsNum, chunkSize)
	if err != nil {
		return 0, xerrors.Errorf("unable to try, err: %w", err)
	}
	endTime := time.Now()
	diff := endTime.Sub(startTime)
	fmt.Printf("threadsNum:%d, chunkSize:%d, calcValsSize:%d, duration:%s\n", threadsNum, chunkSize, calcValsSize(result), diff)
	return diff, nil
}

func main() {
	if len(os.Args) != 3 {
		panic("usage: tool file.jsonl limit")
	}
	filename := os.Args[1]
	limitStr := os.Args[2]
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		panic(err)
	}

	fmt.Printf("maxProcs:%d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("numCPU:%d\n", runtime.NumCPU())

	fmt.Println("Start to load changeItems")
	changeItems, err := getChangeItems(filename, limit)
	fmt.Println("Finish to load changeItems")
	if err != nil {
		panic(err)
	}
	for {
		stat := NewMyStat()
		threadsNums := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512}                                             // #10 items
		chunkSizes := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536} // #17 items
		for _, threadsNum := range threadsNums {
			for _, chunkSize := range chunkSizes {
				type pairType struct {
					threadsNum int
					chunksNum  int
				}
				diff, err := timerWrapper(changeItems, threadsNum, chunkSize)
				if err != nil {
					panic(err)
				}
				stat.AddResult(pairType{threadsNum: threadsNum, chunksNum: chunkSize}, diff)
			}
		}
		stat.Print(16)
	}
}
