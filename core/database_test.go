package jikan

import (
	"os"
	"testing"
	"time"
)

var (
	s1 = []byte{
		10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
		10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
	}
	s2 = []byte{
		20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
		20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
	}
)

func TestDatabaseOpenAndClose(t *testing.T) {
	defer os.Remove("test.db")

	db, err := Open("test.db")
	if err != nil {
		t.Error(err)
	}

	if err := db.Close(); err != nil {
		t.Error(err)
	}
}

func TestDatabaseHeader(t *testing.T) {
	defer os.Remove("test.db")

	db, err := Open("test.db")
	if err != nil {
		t.Error(err)
	}

	defer db.Close()
}

func TestDatabaseNewStream(t *testing.T) {
	defer os.Remove("test.db")

	db, err := Open("test.db")
	if err != nil {
		t.Error(err)
	}

	defer db.Close()

	s, err := db.Stream(s1)
	if err != nil {
		t.Error(err)
	}

	if s == nil {
		t.Errorf("stream shouldn't be nil")
	}
}

func TestDatabaseAddOneRecord(t *testing.T) {
	defer os.Remove("test.db")

	db, err := Open("test.db")
	if err != nil {
		t.Error(err)
	}

	defer db.Close()

	s, err := db.Stream(s1)
	if err != nil {
		t.Error(err)
	}

	f := func(t *StreamTx) error {
		return t.Add(time.Now(), 1)
	}

	if err := s.WithTx(f); err != nil {
		t.Error(err)
	}
}

func TestDatabaseAddTwoRecords(t *testing.T) {
	defer os.Remove("test.db")

	db, err := Open("test.db")
	if err != nil {
		t.Error(err)
	}

	defer db.Close()

	s, err := db.Stream(s1)
	if err != nil {
		t.Error(err)
	}

	f := func(tx *StreamTx) error {
		if err := tx.Add(time.Now(), 1); err != nil {
			return err
		}

		if err := tx.Add(time.Now(), 2); err != nil {
			return err
		}

		return nil
	}

	if err := s.WithTx(f); err != nil {
		t.Error(err)
	}
}

func TestDatabaseBlockExpansion(t *testing.T) {
	defer os.Remove("test.db")

	db, err := Open("test.db")
	if err != nil {
		t.Error(err)
	}

	defer db.Close()

	s, err := db.Stream(s1)
	if err != nil {
		t.Error(err)
	}

	f := func(tx *StreamTx) error {
		for i := 0; i < 1000; i++ {
			if err := tx.Add(time.Now().Add(time.Second*time.Duration(i)), int64(i)); err != nil {
				return err
			}
		}

		return nil
	}

	if err := s.WithTx(f); err != nil {
		t.Error(err)
	}
}

func TestDatabaseIterator(t *testing.T) {
	defer os.Remove("test.db")

	db, err := Open("test.db")
	if err != nil {
		t.Error(err)
	}

	defer db.Close()

	s, err := db.Stream(s1)
	if err != nil {
		t.Error(err)
	}

	f := func(tx *StreamTx) error {
		for i := 0; i < 50; i++ {
			if err := tx.Add(time.Now().Add(time.Second*time.Duration(i)), int64(i)); err != nil {
				return err
			}
		}

		return nil
	}

	if err := s.WithTx(f); err != nil {
		t.Error(err)
	}
}
