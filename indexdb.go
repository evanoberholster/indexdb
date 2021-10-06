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

var (
	ErrDuplicateFound = errors.New("error duplicate key found")
	ErrNotFound       = bptree.ErrNotFound
)

type EncodeFn func() (key []byte, val []byte, err error)
type DecodeFn func(key []byte, val []byte) error

type IndexDB struct {
	bf     *fmap.BlockFile
	bpt    *bptree.BpTree
	config Config
	m      sync.RWMutex
	cf     *cuckoo.ScalableCuckooFilter
}

type Config struct {
	Path             string
	KeySize, ValSize int
}

func indexNotExist(path string) bool {
	_, err := os.Stat(path)
	return errors.Is(err, os.ErrNotExist)
}

func OpenIndexDB(config Config) (*IndexDB, error) {
	var bf *fmap.BlockFile
	var bpt *bptree.BpTree
	var err error
	// If file path doesn't exist -> create
	if indexNotExist(config.Path) {
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

	return &IndexDB{
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

func (i *IndexDB) Size() int {
	return i.bpt.Size()
}

func (i *IndexDB) Close() error {
	i.m.Lock()
	defer i.m.Unlock()
	return i.bf.Close()
}

func (i *IndexDB) Set(key []byte, val []byte) error {
	// Cuckoo Filter
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

func (i *IndexDB) Get(key []byte, decodeFn DecodeFn) error {
	// Cuckoo Filter
	i.m.RLock()
	defer i.m.RUnlock()
	if !i.cf.Lookup(key) {
		return ErrNotFound
	}
	return i.bpt.UnsafeGet(key, decodeFn)
}
