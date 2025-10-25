package dbfile

import "os"

type FileManager struct {
	dbDirectory *os.File
	blockSize   int
}
