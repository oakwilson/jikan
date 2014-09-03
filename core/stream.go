package jikan

import (
	"sync"
	"time"

	"github.com/demizer/go-elog"
	"github.com/facebookgo/stackerr"
)

type Stream struct {
	sync.Mutex

	id    []byte
	db    *Database
	head  *block
	chain []*block
}

func newStream(db *Database, id []byte) (*Stream, error) {
	log.Debugf("creating stream `%s'\n", id)

	head, err := db.getRoot(id)
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	log.Debugf("getting block chain\n")

	chain := []*block{head}

	for head.next != 0 {
		log.Debugf("... next block is at %d\n", head.next)

		head, err = db.getBlock(head.next)

		if err != nil {
			return nil, stackerr.Wrap(err)
		}

		chain = append(chain, head)
	}

	s := Stream{
		id:    id,
		db:    db,
		head:  head,
		chain: chain,
	}

	return &s, nil
}

func (s *Stream) Tx() *StreamTx {
	s.Lock()

	return &StreamTx{s}
}

func (s *Stream) WithTx(fn func(t *StreamTx) error) error {
	t := s.Tx()

	if err := fn(t); err != nil {
		if err := t.Cancel(); err != nil {
			return stackerr.Wrap(err)
		}

		return stackerr.Wrap(err)
	}

	if err := t.Commit(); err != nil {
		return stackerr.Wrap(err)
	} else {
		return nil
	}
}

func (s *Stream) Iterator() *StreamIterator {
	return newStreamIterator(s)
}

func (s *Stream) add(t time.Time, v int64) error {
	if err := s.head.add(t, v); err == nil {
		return nil
	} else if err != ERR_BLOCK_FULL {
		return stackerr.Wrap(err)
	}

	// if we get here, it means we ran out of space. time to allocate some more!

	next, err := s.db.newBlock(s.head.length * 2)
	if err != nil {
		return stackerr.Wrap(err)
	}

	s.head.next = next.position
	if err := s.head.writeAndSwapHeader(); err != nil {
		return stackerr.Wrap(err)
	}

	s.head = next
	s.chain = append(s.chain, s.head)

	if err := s.head.add(t, v); err != nil {
		return stackerr.Wrap(err)
	}

	return nil
}
