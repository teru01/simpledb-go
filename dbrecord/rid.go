package dbrecord

import "fmt"

type RID struct {
	blockNum int
	slot     int
}

func NewRID(blockNum int, slot int) *RID {
	return &RID{blockNum: blockNum, slot: slot}
}

func (r *RID) BlockNum() int {
	return r.blockNum
}

func (r *RID) Slot() int {
	return r.slot
}

func (r *RID) String() string {
	return fmt.Sprintf("[block %d, slot %d]", r.blockNum, r.slot)
}
