package jikan

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"
)

type Iterator struct {
	s *Series
	r io.ByteReader

	good bool

	from time.Time
	to   time.Time

	Time  time.Time
	Value int64
}

func NewIterator(s *Series) *Iterator {
	return &Iterator{
		s: s,
		r: bytes.NewReader(s.data[0:s.offset]),
	}
}

func (i *Iterator) Next() error {
	tdelta, err := binary.ReadVarint(i.r)
	if err != nil {
		i.good = false

		if err == io.EOF {
			return nil
		} else {
			return err
		}
	}

	vdelta, err := binary.ReadVarint(i.r)
	if err != nil {
		i.good = false

		if err == io.EOF {
			return nil
		} else {
			return err
		}
	}

	if i.Time.IsZero() {
		i.Time = time.Unix(tdelta/int64(time.Second), tdelta%int64(time.Second))
	} else {
		i.Time = i.Time.Add(time.Duration(tdelta))
	}

	i.Value = i.Value + vdelta

	i.good = true

	return nil
}

func (i *Iterator) From(t time.Time) *Iterator {
	i.from = t

	return i
}

func (i *Iterator) To(t time.Time) *Iterator {
	i.to = t

	return i
}

func (i *Iterator) Good() bool {
	return i.good
}
