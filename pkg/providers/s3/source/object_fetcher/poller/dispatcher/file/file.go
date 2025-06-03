package file

import "time"

type File struct {
	FileName     string    `json:"file_name"`
	FileSize     int64     `json:"file_size"`
	LastModified time.Time `json:"last_modified"`
}

func NewFile(name string, fileSize int64, lastModified time.Time) *File {
	return &File{
		FileName:     name,
		FileSize:     fileSize,
		LastModified: lastModified,
	}
}
