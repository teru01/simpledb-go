package dbindex

import "github.com/teru01/simpledb-go/dbconstant"

type DirEntry struct {
	value     dbconstant.Constant
	blkNumber int
}

func NewDirEntry(value dbconstant.Constant, blkNumber int) *DirEntry {
	return &DirEntry{
		value:     value,
		blkNumber: blkNumber,
	}
}

func (d *DirEntry) Value() dbconstant.Constant {
	return d.value
}

func (d *DirEntry) BlkNumber() int {
	return d.blkNumber
}
