//go:build !disable_yt_provider

package source

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/providers/yt/iter"
)

type dataObjects struct {
	it  iter.IteratorBase
	obj dataObject
}

func (d dataObjects) Next() bool {
	return d.it.Next()
}

func (d dataObjects) Err() error {
	return nil
}

func (d dataObjects) Close() {
	d.it.Close()
}

func (d dataObjects) Object() (base.DataObject, error) {
	return d.obj, nil
}

func (d dataObjects) ToOldTableMap() (abstract.TableMap, error) {
	return nil, xerrors.New("legacy table map is not supported")
}

type dataObject struct {
	it   iter.IteratorBase
	part dataObjectPart
}

func (d dataObject) Name() string {
	return d.part.Name()
}

func (d dataObject) FullName() string {
	return d.part.FullName()
}

func (d dataObject) Next() bool {
	return d.it.Next()
}

func (d dataObject) Err() error {
	return nil
}

func (d dataObject) Close() {
	d.it.Close()
}

func (d dataObject) Part() (base.DataObjectPart, error) {
	return d.part, nil
}

func (d dataObject) ToOldTableID() (*abstract.TableID, error) {
	return &abstract.TableID{
		Namespace: "",
		Name:      d.FullName(),
	}, nil
}

type dataObjectPart string

func (d dataObjectPart) Name() string {
	return d.FullName()
}

func (d dataObjectPart) FullName() string {
	return string(d)
}

func (d dataObjectPart) ToOldTableDescription() (*abstract.TableDescription, error) {
	return &abstract.TableDescription{
		Name:   d.FullName(),
		Schema: "",
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}, nil
}

func (d dataObjectPart) ToTablePart() (*abstract.TableDescription, error) {
	return d.ToOldTableDescription()
}

func newDataObjects(ID string) dataObjects {
	return dataObjects{
		it: iter.NewSingleshotIter(),
		obj: dataObject{
			it:   iter.NewSingleshotIter(),
			part: dataObjectPart(ID),
		},
	}
}
