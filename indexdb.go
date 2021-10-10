package indexdb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/evanoberholster/indexdb/tree/bptree"
	cuckoo "github.com/seiflotfy/cuckoofilter"
	"github.com/timtadh/fs2/fmap"
)

// Errors
var (
	ErrDuplicateFound = errors.New("error duplicate key found")
	ErrNotFound       = bptree.ErrNotFound
)

type EncodeFn func() (key []byte, val []byte, err error)
type DecodeFn func(key []byte, val []byte) error

// Index is an Index Database that is memory mapped to disk.
type Index struct {
	m      sync.RWMutex
	bf     *fmap.BlockFile
	bpt    *bptree.BpTree
	cf     *cuckoo.ScalableCuckooFilter
	config Config
}

// Config is the configuration for opening an IndexDB
type Config struct {
	Path             string
	KeySize, ValSize int
}

// Open an IndexDB with the given Config
func Open(config Config) (*Index, error) {
	var bf *fmap.BlockFile
	var bpt *bptree.BpTree
	var err error
	// If file path doesn't exist -> create
	if fileNotExist(config.Path) {
		bf, err = fmap.CreateBlockFile(config.Path)
		if err != nil {
			return nil, err
		}
		bpt, err = bptree.New(bf, config.KeySize, config.ValSize)
		if err != nil {
			defer bf.Close()
			return nil, err
		}
	} else {
		bf, err = fmap.OpenBlockFile(config.Path)
		if err != nil {
			return nil, err
		}
		bpt, err = bptree.Open(bf)
		if err != nil {
			defer bf.Close()
			return nil, err
		}
	}
	// Cuckoo Filter
	return &Index{
		cf:     buildCuckooFilter(bpt),
		bf:     bf,
		bpt:    bpt,
		config: config,
	}, nil
}
func buildCuckooFilter(bpt *bptree.BpTree) *cuckoo.ScalableCuckooFilter {
	start := time.Now()
	defer fmt.Println(time.Since(start), " Cuckoo Build Time")
	cf := cuckoo.NewScalableCuckooFilter()
	bpt.DoIterate(func(key []byte, value []byte) error {
		cf.InsertUnique(key)
		return nil
	})
	return cf
}

// Size returns the number of keys set.
func (i *Index) Size() int {
	return i.bpt.Size()
}

// Close the Index
func (i *Index) Close() error {
	i.m.Lock()
	defer i.m.Unlock()
	return i.bf.Close()
}

// Set a key and value to the IndexDB.
func (i *Index) Set(key []byte, val []byte) error {
	i.m.Lock()
	defer i.m.Unlock()
	if !i.cf.Lookup(key) {
		return i.bpt.Add(key, val)
	}
	err := i.bpt.UnsafeGet(key, func(key, value []byte) error {
		copy(val, value)
		return ErrDuplicateFound
	})
	if err == nil {
		return i.bpt.Add(key, val)
	}
	return err
}

// Get a key from the Index and process with the decodeFn.
// Uses an unsafeGet.
func (i *Index) Get(key []byte, decodeFn DecodeFn) error {
	i.m.RLock()
	defer i.m.RUnlock()
	if !i.cf.Lookup(key) {
		return ErrNotFound
	}
	return i.bpt.UnsafeGet(key, decodeFn)
}

// Remove a key from the Index
func (i *Index) Remove(key []byte) error {
	i.m.Lock()
	defer i.m.Unlock()
	if i.cf.Delete(key) {
		i.bpt.Remove(key, func(b []byte) bool { return true })
	}
	return ErrNotFound
}

func fileNotExist(path string) bool {
	_, err := os.Stat(path)
	return errors.Is(err, os.ErrNotExist)
}
