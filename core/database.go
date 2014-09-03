package jikan

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"

	"github.com/demizer/go-elog"
	"github.com/edsrzf/mmap-go"
	"github.com/facebookgo/stackerr"
)

const MINIMUM_HEADER_LENGTH = 33

type dbRoot struct {
	id       []byte
	position uint64
}

type dbStream struct {
	id     []byte
	stream *Stream
}

type Database struct {
	sync.RWMutex

	filename string
	fd       *os.File
	mm       mmap.MMap

	page  uint8
	index uint64
	used  uint64

	roots   []*dbRoot
	streams []*dbStream
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
		fd: fd,
		mm: mm,
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
		log.Debugf("locking stream `%s'...\n", s.id)

		wg.Add(1)
		go func() {
			s.stream.Lock()
			wg.Done()

			log.Debugf("locked stream `%s'\n", s.id)
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
	id := make([]byte, len(name))
	copy(id, name)

	log.Debugf("getting stream `%s'\n", id)

	for _, s := range db.streams {
		if bytes.Equal(s.id, name) {
			log.Debugf("fetching cached stream\n")

			return s.stream, nil
		}
	}

	if stream, err := newStream(db, id); err != nil {
		return nil, stackerr.Wrap(err)
	} else {
		db.streams = append(db.streams, &dbStream{
			id:     id,
			stream: stream,
		})

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

func (db *Database) getRoot(id []byte) (*block, error) {
	log.Debugf("getting root block `%s'\n", id)

	for _, r := range db.roots {
		if bytes.Equal(r.id, id) {
			log.Debugf("found root position: %d\n", r.position)

			if root, err := db.getBlock(r.position); err != nil {
				return nil, stackerr.Wrap(err)
			} else {
				return root, nil
			}
		}
	}

	log.Debugf("creating new root block\n")

	if root, err := db.newBlock(32); err != nil {
		return nil, stackerr.Wrap(err)
	} else {
		db.roots = append(db.roots, &dbRoot{
			id:       id,
			position: root.position,
		})

		if err := db.writeAndSwapHeader(); err != nil {
			return nil, stackerr.Wrap(err)
		}

		return root, nil
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

func (db *Database) readIndex(position uint64) ([]*dbRoot, error) {
	log.Debugf("reading index from %d\n", position)

	if position == 0 {
		return nil, nil
	}

	if int(position) >= len(db.mm) {
		return nil, stackerr.New("position was out of bounds")
	}

	count := int(binary.BigEndian.Uint32(db.mm[position : position+4]))

	roots := make([]*dbRoot, count, count)

	o := int(position) + 4
	for i := 0; i < count; i++ {
		log.Debugf("reading root %d/%d from offset %d\n", i, count, o)

		if o >= len(db.mm) {
			return nil, stackerr.New("stream id length overruns bounds")
		}
		streamIdSize := int(binary.BigEndian.Uint16(db.mm[o : o+2]))

		log.Debugf("id size is %d\n", streamIdSize)

		if o+streamIdSize >= len(db.mm) {
			return nil, stackerr.New("stream id overruns bounds")
		}
		streamId := make([]byte, streamIdSize)
		copy(streamId, db.mm[o+2:o+2+streamIdSize])

		if o >= len(db.mm) {
			return nil, stackerr.New("stream position data overruns bounds")
		}
		streamPosition := binary.BigEndian.Uint64(db.mm[o+2+streamIdSize : o+2+streamIdSize+8])

		o += 2 + streamIdSize + 8

		log.Debugf("adding root `%s' at %d\n", streamId, streamPosition)

		roots[i] = &dbRoot{
			id:       streamId,
			position: streamPosition,
		}
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
	log.Debugf("writing database index to position %d\n", position)

	requiredLength := 0
	for _, r := range db.roots {
		requiredLength += 2 + len(r.id) + 8
	}

	length := 0

	if position != 0 {
		length = int(binary.BigEndian.Uint32(db.mm[int(position) : int(position)+4]))
	}

	if length < requiredLength+4 {
		length = requiredLength + 4

		if newPosition, err := db.allocate(uint64(length)); err != nil {
			return 0, stackerr.Wrap(err)
		} else if newPosition != position {
			position = newPosition
		}
	}

	index := db.mm[int(position) : int(position)+length]

	log.Debugf("writing index root count of %d\n", len(db.roots))

	binary.BigEndian.PutUint32(index[0:4], uint32(len(db.roots)))

	o := 4
	for _, v := range db.roots {
		log.Debugf("writing root record `%s' at offset %d\n", v.id, o)

		binary.BigEndian.PutUint16(index[o:o+2], uint16(len(v.id)))
		copy(index[o+2:o+2+len(v.id)], v.id)
		binary.BigEndian.PutUint64(index[o+2+len(v.id):o+2+len(v.id)+8], v.position)

		o += 2 + len(v.id) + 8
	}

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

	log.Debugf("allocated at %d, total %d\n", o, db.used)

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
