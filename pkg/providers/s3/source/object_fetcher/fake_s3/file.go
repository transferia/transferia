package fake_s3

import "time"

// TODO - maybe remove it in advance to dispatcher.File ?

type File struct {
	FileName     string
	Body         []byte
	LastModified time.Time
}

func NewFile(fileName string, body []byte, ns int64) *File {
	return &File{
		FileName:     fileName,
		Body:         body,
		LastModified: time.Unix(0, ns),
	}
}
