package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"oakwilson.com/p/jikan"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	s, err := jikan.NewSeries("demo.db")
	if err != nil {
		log.Fatal(err)
	}

	err = s.Locked(func() error {
		for i := 0; i < 10; i++ {
			if err := s.Add(time.Now(), rand.Int63n(100)); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	for it := s.Range(); it.Good(); it.Next() {
		fmt.Printf("%s\t%d\n", it.Time.Format(time.RFC3339Nano), it.Value)
	}
}
