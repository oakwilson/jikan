package jikan

import (
	"encoding/binary"
	"time"

	"github.com/demizer/go-elog"
)

type StreamIterator struct {
	str *Stream
	idx int
	pos int

	good bool

	from time.Time
	to   time.Time

	Time  time.Time
	Value int64
}

func newStreamIterator(s *Stream) *StreamIterator {
	log.Debugf("constructing new iterator\n")

	i := StreamIterator{
		str:  s,
		good: true,
	}

	i.Next()

	return &i
}

func (i *StreamIterator) Next() error {
START:
	if i.idx >= len(i.str.chain) {
		i.good = false

		return nil
	}

	blk := i.str.chain[i.idx]

	log.Debugf("moving to next item\n")
	log.Debugf("idx %d, pos %d/%d/%d, good %#v\n", i.idx, i.pos, blk.used, blk.length, i.good)

	if uint32(i.pos) == blk.used {
		i.idx++
		i.pos = 0

		i.Time = time.Time{}
		i.Value = 0

		// can't just fall through here, in case the next block has been allocated
		// but is also empty. weird edge case, but it's possible...
		goto START
	}

	tdelta, n := binary.Varint(blk.db.mm[blk.position+93+uint64(i.pos) : blk.position+93+uint64(blk.used)])
	i.pos += n
	vdelta, n := binary.Varint(blk.db.mm[blk.position+93+uint64(i.pos) : blk.position+93+uint64(blk.used)])
	i.pos += n

	if i.Time.IsZero() {
		i.Time = time.Unix(tdelta/int64(time.Millisecond), tdelta%int64(time.Millisecond))
	} else {
		i.Time = i.Time.Add(time.Duration(tdelta))
	}

	i.Value = i.Value + vdelta

	i.good = true

	return nil
}

func (i *StreamIterator) From(t time.Time) *StreamIterator {
	i.from = t

	return i
}

func (i *StreamIterator) To(t time.Time) *StreamIterator {
	i.to = t

	return i
}

func (i *StreamIterator) Good() bool {
	return i.good
}
