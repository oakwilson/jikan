package jikan

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

type Series struct {
	fd        *os.File
	mm        mmap.MMap
	data      []byte
	page      uint8
	offset    uint64
	lastTime  time.Time
	lastValue int64
	lock      sync.RWMutex
}

func NewSeries(filename string) (*Series, error) {
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	mm, err := mmap.Map(fd, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	s := Series{
		fd:   fd,
		mm:   mm,
		data: mm[81:],
		page: mm[0],
	}

	s.readPage(s.page)

	return &s, nil
}

func (s *Series) String() string {
	return fmt.Sprintf("Series{page=%#v offset=%#v lastTime=%#v lastValue=%#v}", s.page, s.offset, s.lastTime, s.lastValue)
}

func (s *Series) Locked(f func() error) error {
	s.Lock()
	defer s.Unlock()

	if err := f(); err != nil {
		return err
	}

	if err := s.writeAndSwapPage(); err != nil {
		return err
	}

	return nil
}

func (s *Series) Lock() {
	s.lock.Lock()
}

func (s *Series) Unlock() {
	s.lock.Unlock()
}

func (s *Series) Add(t time.Time, v int64) error {
	td := t.Sub(s.lastTime)
	vd := v - s.lastValue

	s.offset += uint64(binary.PutVarint(s.data[s.offset:], int64(td)))
	s.offset += uint64(binary.PutVarint(s.data[s.offset:], vd))

	s.lastTime = t
	s.lastValue = v

	return nil
}

func (s *Series) Range() *Iterator {
	it := NewIterator(s)

	it.Next()

	return it
}

func (s *Series) readPage(page uint8) {
	offset := (page * 40) + 1

	s.offset = binary.BigEndian.Uint64(s.mm[offset:])
	lastTime, _ := binary.Varint(s.mm[offset+8:])
	lastValue, _ := binary.Varint(s.mm[offset+24:])

	s.lastTime = time.Unix(lastTime/int64(time.Second), lastTime%int64(time.Second))
	s.lastValue = lastValue
}

func (s *Series) writePage(page uint8) {
	offset := (page * 40) + 1

	binary.BigEndian.PutUint64(s.mm[offset:], s.offset)
	binary.PutVarint(s.mm[offset+8:], s.lastTime.UnixNano())
	binary.PutVarint(s.mm[offset+24:], s.lastValue)
}

func (s *Series) writeAndSwapPage() error {
	s.writePage(s.page ^ 1)
	s.mm[0] ^= 1
	return s.mm.Flush()
}
