package dbfile

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const defaultDirectory = "/tmp/simpledb"

type FileManager struct {
	mu          sync.Mutex
	dbDirectory *os.File
	blockSize   int
	isNew       bool
	openFiles   map[string]*os.File
}

func NewFileManager(dbDirectory *os.File, blockSize int) (*FileManager, error) {
	var isNew bool

	if dbDirectory == nil {
		isNew = true
		if err := os.MkdirAll(defaultDirectory, 0755); err != nil {
			return nil, err
		}

		var err error
		dbDirectory, err = os.Open(defaultDirectory)
		if err != nil {
			return nil, fmt.Errorf("failed to open directory %s: %w", defaultDirectory, err)
		}
	} else {
		files, err := dbDirectory.ReadDir(0)
		if err != nil {
			return nil, fmt.Errorf("failed to read directory %s: %w", dbDirectory.Name(), err)
		}
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "temp") {
				if err := os.Remove(filepath.Join(dbDirectory.Name(), file.Name())); err != nil {
					return nil, fmt.Errorf("failed to remove file %s: %w", file.Name(), err)
				}
			}
		}
	}

	return &FileManager{
		dbDirectory: dbDirectory,
		blockSize:   blockSize,
		isNew:       isNew,
		openFiles:   make(map[string]*os.File),
	}, nil
}

func (fm *FileManager) Read(blockID BlockID, p Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	file, err := fm.getFile(blockID.FileName())
	if err != nil {
		return fmt.Errorf("failed to get file %s: %w", blockID.FileName(), err)
	}
	_, err = file.Seek(int64(blockID.BlockNum()*fm.blockSize), io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek file %s: %w", blockID.FileName(), err)
	}
	_, err = file.Read(p.pageBuffer().buffer)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", blockID.FileName(), err)
	}
	return nil
}

func (fm *FileManager) Write(blockID BlockID, p Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	file, err := fm.getFile(blockID.FileName())
	if err != nil {
		return fmt.Errorf("failed to get file %s: %w", blockID.FileName(), err)
	}
	_, err = file.Seek(int64(blockID.BlockNum()*fm.blockSize), io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek file %s: %w", blockID.FileName(), err)
	}
	_, err = file.Write(p.pageBuffer().buffer)
	if err != nil {
		return fmt.Errorf("failed to write file %s: %w", blockID.FileName(), err)
	}
	return nil
}

// fileNameのファイルを1ブロック伸ばす
func (fm *FileManager) Append(fileName string) (BlockID, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	blockNum, err := fm.FileBlockLength(fileName)
	if err != nil {
		return BlockID{}, fmt.Errorf("failed to get file block length %s: %w", fileName, err)
	}
	newBlockID := NewBlockID(fileName, blockNum)

	b := make([]byte, fm.blockSize)
	f, err := fm.getFile(fileName)
	if err != nil {
		return BlockID{}, fmt.Errorf("failed to get file %s: %w", fileName, err)
	}
	_, err = f.Seek(int64(newBlockID.blockNum*fm.blockSize), io.SeekStart)
	if err != nil {
		return BlockID{}, fmt.Errorf("failed to seek file %s: %w", fileName, err)
	}
	_, err = f.Write(b)
	if err != nil {
		return BlockID{}, fmt.Errorf("failed to write file %s: %w", fileName, err)
	}
	return newBlockID, nil
}

// fileNameのファイルのブロック数を取得.ブロック単位で書き込まれるので切り捨てても問題ない
func (fm *FileManager) FileBlockLength(fileName string) (int, error) {
	file, err := fm.getFile(fileName)
	if err != nil {
		return 0, fmt.Errorf("failed to get file %s: %w", fileName, err)
	}
	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info %s: %w", fileName, err)
	}
	return int(info.Size() / int64(fm.blockSize)), nil
}

func (fm *FileManager) IsNew() bool {
	return fm.isNew
}

// fixed value
func (fm *FileManager) BlockSize() int {
	return fm.blockSize
}

func (fm *FileManager) getFile(fileName string) (*os.File, error) {
	file, ok := fm.openFiles[fileName]
	if !ok {
		f, err := os.OpenFile(filepath.Join(fm.dbDirectory.Name(), fileName), os.O_RDWR|os.O_CREATE|os.O_SYNC, 0644)
		if err != nil {
			return nil, err
		}
		fm.openFiles[fileName] = f
		return f, nil
	}
	return file, nil
}
