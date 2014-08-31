package jikan

import (
	"crypto/sha1"
	"encoding/binary"
	"os"
	"sync"

	"github.com/demizer/go-elog"
	"github.com/edsrzf/mmap-go"
	"github.com/facebookgo/stackerr"
)

const MINIMUM_HEADER_LENGTH = 33

type Database struct {
	sync.RWMutex

	filename string
	fd       *os.File
	mm       mmap.MMap

	page    uint8
	index   uint64
	used    uint64
	roots   map[[20]byte]uint64
	streams map[[20]byte]*Stream
}

func Open(filename string) (*Database, error) {
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	if stat.Size() < MINIMUM_HEADER_LENGTH {
		if err := fd.Truncate(MINIMUM_HEADER_LENGTH); err != nil {
			return nil, stackerr.Wrap(err)
		}
	}

	mm, err := mmap.Map(fd, mmap.RDWR, 0)
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	db := Database{
		fd:      fd,
		mm:      mm,
		streams: make(map[[20]byte]*Stream),
	}

	page := mm[0]

	if err := db.readHeader(page); err != nil {
		return nil, stackerr.Wrap(err)
	}

	db.page = page

	// a new database has the "used" field set to 0, but the minimum header size
	// is actually 33 bytes (a one byte page id and two 16 byte index/used pairs)
	if db.used == 0 {
		db.used = MINIMUM_HEADER_LENGTH
	}

	return &db, nil
}

func (db *Database) Close() error {
	var wg sync.WaitGroup

	log.Debugf("adding locks for open streams\n")

	for _, s := range db.streams {
		log.Debugf("locking stream `%x'...\n", s.id)

		wg.Add(1)
		go func() {
			s.Lock()
			wg.Done()

			log.Debugf("locked stream `%x'\n", s.id)
		}()
	}

	log.Debugf("waiting for locks...\n")

	wg.Wait()

	log.Debugf("locks acquired! closing time!\n")

	if err := db.mm.Unmap(); err != nil {
		return stackerr.Wrap(err)
	}

	if err := db.fd.Close(); err != nil {
		return stackerr.Wrap(err)
	}

	return nil
}

func (db *Database) writeAndSwapHeader() error {
	log.Debugf("writing/swapping database header\n")

	db.Lock()
	defer db.Unlock()

	if err := db.writeHeader(db.page ^ 1); err != nil {
		return stackerr.Wrap(err)
	}

	log.Debugf("before swap: %d/%d\n", db.page, db.mm[0])

	db.page ^= 1
	db.mm[0] = db.page

	log.Debugf("after swap: %d/%d\n", db.page, db.mm[0])

	return db.mm.Flush()
}

func (db *Database) withLock(fn func() error) error {
	db.Lock()
	defer db.Unlock()

	if err := fn(); err != nil {
		return stackerr.Wrap(err)
	}

	return nil
}

func (db *Database) Stream(name []byte) (*Stream, error) {
	var id [20]byte

	h := sha1.New()
	h.Write(name)
	copy(id[:], h.Sum(nil))

	log.Debugf("getting stream `%s' (`%x')\n", name, id)

	if stream, ok := db.streams[id]; ok {
		log.Debugf("fetching cached stream\n")

		return stream, nil
	}

	if stream, err := newStream(db, id); err != nil {
		return nil, stackerr.Wrap(err)
	} else {
		db.streams[id] = stream

		return stream, nil
	}
}

func (db *Database) getBlock(position uint64) (*block, error) {
	log.Debugf("getting block at %d\n", position)

	if b, err := newBlock(db, position); err != nil {
		return nil, stackerr.Wrap(err)
	} else {
		return b, nil
	}
}

func (db *Database) newBlock(size uint32) (*block, error) {
	log.Debugf("creating block of %d bytes\n", size)

	position, err := db.allocate(93 + uint64(size))
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	// explicitly zero out the header before block creation in case we're re-using
	// reclaimed or previously-failed space
	for i := 0; i < 93; i++ {
		db.mm[int(position)+i] = 0
	}

	binary.BigEndian.PutUint32(db.mm[position:position+4], size)

	if b, err := db.getBlock(position); err != nil {
		return nil, stackerr.Wrap(err)
	} else {
		return b, nil
	}
}

func (db *Database) getRoot(id [20]byte) (*block, error) {
	log.Debugf("getting root block `%x'\n", id)

	if position, ok := db.roots[id]; ok {
		log.Debugf("found root position: %d\n", position)

		if root, err := db.getBlock(position); err != nil {
			return nil, stackerr.Wrap(err)
		} else {
			return root, nil
		}
	} else {
		log.Debugf("creating new root block\n")

		if root, err := db.newBlock(32); err != nil {
			return nil, stackerr.Wrap(err)
		} else {
			db.roots[id] = root.position

			if err := db.writeAndSwapHeader(); err != nil {
				return nil, stackerr.Wrap(err)
			}

			return root, nil
		}
	}
}

func (db *Database) readHeader(page byte) error {
	log.Debugf("reading database header from page %d\n", page)

	o := 1 + (page * 16)

	index := binary.BigEndian.Uint64(db.mm[o : o+8])
	used := binary.BigEndian.Uint64(db.mm[o+8 : o+16])

	log.Debugf("index %d, used %d\n", index, used)

	roots, err := db.readIndex(index)
	if err != nil {
		return stackerr.Wrap(err)
	}

	db.index = index
	db.used = used
	db.roots = roots

	return nil
}

func (db *Database) readIndex(position uint64) (map[[20]byte]uint64, error) {
	log.Debugf("reading index from %d\n", position)

	roots := make(map[[20]byte]uint64)

	if position == 0 {
		return roots, nil
	}

	if int(position) >= len(db.mm) {
		return nil, stackerr.New("position was out of bounds")
	}

	length := int(binary.BigEndian.Uint32(db.mm[position : position+4]))
	used := int(binary.BigEndian.Uint32(db.mm[position+4 : position+8]))

	log.Debugf("index is %d bytes long with %d bytes used\n", length, used)

	if int(position)+used > len(db.mm) {
		return nil, stackerr.New("index apparently overruns bounds")
	}

	records := used / 28

	for i := 0; i < records; i++ {
		eoff := 8 + int(position) + i*28

		var k [20]byte
		copy(k[0:20], db.mm[eoff:eoff+20])

		log.Debugf("adding root `%x'\n", k)

		roots[k] = binary.BigEndian.Uint64(db.mm[eoff+20 : eoff+28])
	}

	return roots, nil
}

func (db *Database) writeHeader(page byte) error {
	log.Debugf("writing database header to page %d\n", page)

	if newIndex, err := db.writeIndex(db.index); err != nil {
		return stackerr.Wrap(err)
	} else if db.index != newIndex {
		db.index = newIndex
	}

	log.Debugf("index %d, used %d\n", db.index, db.used)

	o := 1 + (page * 16)

	binary.BigEndian.PutUint64(db.mm[o:o+8], db.index)
	binary.BigEndian.PutUint64(db.mm[o+8:o+16], db.used)

	return nil
}

func (db *Database) writeIndex(position uint64) (uint64, error) {
	length := 0

	if position != 0 {
		length = int(binary.BigEndian.Uint32(db.mm[int(position) : int(position)+4]))
	}

	if length < len(db.roots)*28+8 {
		length = len(db.roots)*28*2 + 8

		if newPosition, err := db.allocate(uint64(length)); err != nil {
			return 0, stackerr.Wrap(err)
		} else if newPosition != position {
			position = newPosition
		}
	}

	index := db.mm[int(position) : int(position)+length]
	data := index[8:length]

	i := 0
	for k, v := range db.roots {
		o := i * 28
		record := data[o : o+28]
		copy(record[0:20], k[0:20])
		binary.BigEndian.PutUint64(record[20:28], v)
		i++
	}

	binary.BigEndian.PutUint32(index[0:4], uint32(length))
	binary.BigEndian.PutUint32(index[4:8], uint32(len(db.roots)*28))

	return position, nil
}

func (db *Database) allocate(size uint64) (uint64, error) {
	log.Debugf("allocating %d bytes\n", size)

	if len(db.mm)-int(db.used) < int(size) {
		if err := db.expand(size); err != nil {
			return 0, stackerr.Wrap(err)
		}
	}

	o := db.used

	db.used += size

	log.Debugf("allocated at %d, total %d\n", size, db.used)

	return o, nil
}

func (db *Database) expand(size uint64) error {
	length := len(db.mm)

	if err := db.mm.Unmap(); err != nil {
		return stackerr.Wrap(err)
	}

	if err := db.fd.Truncate(int64(length + int(size))); err != nil {
		return stackerr.Wrap(err)
	}

	mm, err := mmap.Map(db.fd, mmap.RDWR, 0)
	if err != nil {
		return stackerr.Wrap(err)
	}

	db.mm = mm

	return nil
}
