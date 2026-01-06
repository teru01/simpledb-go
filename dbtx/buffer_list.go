package dbtx

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
)

type BufferList struct {
	buffers       map[dbfile.BlockID]*dbbuffer.Buffer
	pinsCount     map[dbfile.BlockID]int
	bufferManager *dbbuffer.BufferManager
}

func NewBufferList(bm *dbbuffer.BufferManager) *BufferList {
	return &BufferList{
		buffers:       make(map[dbfile.BlockID]*dbbuffer.Buffer),
		pinsCount:     make(map[dbfile.BlockID]int),
		bufferManager: bm,
	}
}

func (b *BufferList) Buffer(blk dbfile.BlockID) (*dbbuffer.Buffer, error) {
	if buf, ok := b.buffers[blk]; ok {
		return buf, nil
	}
	return nil, fmt.Errorf("no such block on buffer %s", blk)
}

func (b *BufferList) Pin(ctx context.Context, blk dbfile.BlockID) error {
	buf, err := b.bufferManager.Pin(ctx, blk)
	if err != nil {
		return fmt.Errorf("failed to pin: %w", err)
	}
	b.buffers[blk] = buf
	b.pinsCount[blk]++
	return nil
}

func (b *BufferList) UnPin(blk dbfile.BlockID) error {
	buf, err := b.Buffer(blk)
	if err != nil {
		return fmt.Errorf("failed to unpin: %w", err)
	}
	b.bufferManager.Unpin(buf)
	b.pinsCount[blk]--
	if b.pinsCount[blk] <= 0 {
		delete(b.buffers, blk)
		delete(b.pinsCount, blk)
	}
	return nil
}

func (b *BufferList) UnpinAll() {
	for blk, count := range b.pinsCount {
		for range count {
			if err := b.UnPin(blk); err != nil {
				slog.Error(err.Error())
				continue // 存在しないやつは無視して全部unpinする
			}
		}
	}
	clear(b.buffers)
	clear(b.pinsCount)
}
