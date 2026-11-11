package s3

type parquetRow struct {
	ID      int64  `parquet:"id"`
	Payload string `parquet:"payload"`
}
