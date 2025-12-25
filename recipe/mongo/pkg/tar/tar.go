package tarutil

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

func TarXzf(path string) (*tar.Reader, func() error, error) {
	dummyFunc := func() error { return nil }
	f, err := os.Open(path)
	if err != nil {
		return nil, dummyFunc, xerrors.Errorf("cannot open file '%s': %w", path, err)
	}
	gzw, err := gzip.NewReader(f)
	if err != nil {
		return nil, dummyFunc, xerrors.Errorf("cannot open gzip file '%s': %w", path, err)
	}
	return tar.NewReader(gzw), func() error {
		err := f.Close()
		if err != nil {
			return err
		}
		return nil
	}, err
}

func UnpackArchive(archivePath, destinationPath string) error {
	tarReader, streamClose, err := TarXzf(archivePath)
	if err != nil {
		return xerrors.Errorf("cannot open tar stream: %v", err)
	}
	defer func() {
		err := streamClose()
		if err != nil {
			fmt.Printf("error occured on metaarchive stream close '%s': %v\n", archivePath, err)
		}
	}()

	return UnpackTar(tarReader, destinationPath, func(filename string) bool { return true })
}

func UnpackTar(tarReader *tar.Reader, destinationPath string, predicate func(string) bool) error {
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return xerrors.Errorf("next failed: %w", err)
		}

		if predicate != nil && !predicate(header.Name) {
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir:
			dirPath := path.Join(destinationPath, header.Name)
			if err := os.MkdirAll(dirPath, 0755); err != nil {
				return xerrors.Errorf("cannot create directory: %w", err)
			}
		case tar.TypeReg:
			fullPath := path.Join(destinationPath, header.Name)
			if err := os.MkdirAll(path.Dir(fullPath), 0755); err != nil {
				return xerrors.Errorf("cannot create directory: %w", err)
			}
			err := func() error {
				outFile, err := os.OpenFile(fullPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
				if err != nil {
					return xerrors.Errorf("cannot create file: %w", err)
				}
				defer outFile.Close()
				if _, err := io.Copy(outFile, tarReader); err != nil {
					return xerrors.Errorf("cannot copy contents to file: %w", err)
				}
				return nil
			}()
			if err != nil {
				fmt.Printf("file '%s' extraction failure: %v\n", header.Name, err)
			}
		}
	}
	return nil
}
