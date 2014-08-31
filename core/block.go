package jikan

import (
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/demizer/go-elog"
	"github.com/facebookgo/stackerr"
)

type block struct {
	sync.Mutex

	db       *Database
	position uint64

	length uint32
	page   uint8

	used  uint32
	next  uint64
	time  time.Time
	value int64
}

var (
	ERR_BLOCK_FULL = errors.New("no space left")
)

func newBlock(db *Database, position uint64) (*block, error) {
	log.Debugf("constructing new block at %d\n", position)

	b := block{
		db:       db,
		position: position,
	}

	length := binary.BigEndian.Uint32(db.mm[position : position+4])
	page := db.mm[position+4]

	if err := b.readHeader(page); err != nil {
		return nil, stackerr.Wrap(err)
	}

	b.length = length
	b.page = page

	log.Debugf("length %d, page %d\n", length, page)

	return &b, nil
}

func (b *block) tx(fn func() error) error {
	b.Lock()
	defer b.Unlock()

	if err := fn(); err != nil {
		return stackerr.Wrap(err)
	}

	if err := b.writeAndSwapHeader(); err != nil {
		return stackerr.Wrap(err)
	}

	return nil
}

func (b *block) writeAndSwapHeader() error {
	log.Debugf("writing/swapping block header\n")

	b.Lock()
	defer b.Unlock()

	if err := b.writeHeader(b.page ^ 1); err != nil {
		return stackerr.Wrap(err)
	}

	log.Debugf("before swap: %d/%d\n", b.page, b.db.mm[int(b.position)+4])

	b.page ^= 1
	b.db.mm[int(b.position)+4] = b.page

	log.Debugf("after swap: %d/%d\n", b.page, b.db.mm[int(b.position)+4])

	return b.db.mm.Flush()
}

func (b *block) add(t time.Time, v int64) error {
	td := t.Sub(b.time)
	vd := v - b.value

	u := 0
	var buf [32]byte
	u += binary.PutVarint(buf[u:], int64(td)/1000)
	u += binary.PutVarint(buf[u:], vd)

	if int(b.used)+u >= int(b.length) {
		return ERR_BLOCK_FULL
	}

	copy(b.db.mm[int(b.position)+93+int(b.used):int(b.position)+93+int(b.used)+u], buf[0:u])
	b.used += uint32(u)

	b.time = t
	b.value = v

	return nil
}

func (b *block) readHeader(page uint8) error {
	o := int(b.position) + 5 + int(page*12)

	log.Debugf("reading block header from page %d (offset %d/0x%x)\n", page, o, o)

	d := b.db.mm[o : o+44]

	t, _ := binary.Uvarint(d[12:28])
	v, _ := binary.Varint(d[28:44])

	b.used = binary.BigEndian.Uint32(d[0:4])
	b.next = binary.BigEndian.Uint64(d[4:12])
	b.time = time.Unix(int64(t)/int64(time.Millisecond), int64(t)%int64(time.Millisecond))
	b.value = v

	log.Debugf("used %d, next %d, time %s, value %d\n", b.used, b.next, b.time, b.value)

	return nil
}

func (b *block) writeHeader(page uint8) error {
	o := int(b.position) + 5 + int(page*12)

	log.Debugf("writing block header to page %d (offset %d/0x%x)\n", page, o, o)

	d := b.db.mm[o : o+44]

	binary.BigEndian.PutUint32(d[0:4], b.used)
	binary.BigEndian.PutUint64(d[4:12], b.next)
	binary.PutUvarint(d[12:28], uint64(b.time.UnixNano()))
	binary.PutVarint(d[28:44], b.value)

	log.Debugf("used %d, next %d, time %s, value %d\n", b.used, b.next, b.time, b.value)

	return nil
}
