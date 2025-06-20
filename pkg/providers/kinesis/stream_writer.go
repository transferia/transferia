//go:build !disable_kinesis_provider

package kinesis

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func PutRecord(src *KinesisSource, data []byte, key string) error {
	client, err := NewClient(src)
	if err != nil {
		return xerrors.Errorf("No stream exists with the provided name: %w", err)
	}

	if _, err = client.
		DescribeStream(
			&kinesis.DescribeStreamInput{
				StreamName: &src.Stream,
			}); err != nil {
		return xerrors.Errorf("No stream exists with the provided name: %w", err)
	}
	// put data to stream
	_, err = client.PutRecord(&kinesis.PutRecordInput{
		Data:         []byte(data),
		StreamName:   &src.Stream,
		PartitionKey: aws.String(key),
	})
	if err != nil {
		return xerrors.Errorf("Failed to put records in stream: %w", err)
	}

	return nil
}
