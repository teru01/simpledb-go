package dbfile

import (
	"fmt"
	"hash/fnv"
)

type BlockID struct {
	fileName string
	blockNum int
}

func NewBlockID(fileName string, blockNum int) BlockID {
	return BlockID{fileName: fileName, blockNum: blockNum}
}

func (id BlockID) FileName() string {
	return id.fileName
}

func (id BlockID) BlockNum() int {
	return id.blockNum
}

func (id BlockID) Equals(other BlockID) bool {
	return id.fileName == other.fileName && id.blockNum == other.blockNum
}

func (id BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", id.fileName, id.blockNum)
}

func (id BlockID) Hash() int {
	h := fnv.New64a()
	h.Write([]byte(id.String()))
	return int(h.Sum64())
}
