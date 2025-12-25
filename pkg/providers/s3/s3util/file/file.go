package file

import (
	"encoding/json"
	"time"
)

type File struct {
	FileName       string `json:"file_name"`
	FileSize       int64  `json:"file_size"`
	LastModifiedNS int64  `json:"last_modified_ns"`
}

func (f *File) String() string {
	resultBytes, _ := json.Marshal(f)
	return string(resultBytes)
}

func NewFile(name string, fileSize int64, lastModified time.Time) *File {
	return &File{
		FileName:       name,
		FileSize:       fileSize,
		LastModifiedNS: lastModified.UTC().UnixNano(),
	}
}

//---

type FileArr []File

func (f FileArr) ToBatchingLines() []string {
	result := make([]string, 0, len(f))
	for _, file := range f {
		result = append(result, file.FileName)
	}
	return result
}
