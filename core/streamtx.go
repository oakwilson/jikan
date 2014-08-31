package jikan

import (
	"time"

	"github.com/facebookgo/stackerr"
)

type StreamTx struct {
	s *Stream
}

func (s *StreamTx) Add(t time.Time, v int64) error {
	if err := s.s.add(t, v); err != nil {
		return stackerr.Wrap(err)
	} else {
		return nil
	}
}

func (s *StreamTx) Commit() error {
	defer s.s.Unlock()

	if err := s.s.head.writeAndSwapHeader(); err != nil {
		return stackerr.Wrap(err)
	} else {
		return nil
	}
}

func (s *StreamTx) Cancel() error {
	s.s.Unlock()

	return nil
}
