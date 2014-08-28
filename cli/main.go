package main

import (
	"encoding/csv"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/codegangsta/cli"
	"oakwilson.com/p/jikan"
)

func exportAction(c *cli.Context) {
	s, err := jikan.NewSeries(c.Args().Get(0))
	if err != nil {
		log.Fatal(err)
	}

	var outf io.WriteCloser

	outfile := c.Args().Get(1)
	if outfile == "" || outfile == "-" {
		outf = os.Stdout
	} else {
		if outf, err = os.OpenFile(outfile, os.O_CREATE|os.O_WRONLY, 0644); err != nil {
			log.Fatal(err)
		}
	}

	w := csv.NewWriter(outf)

	for it := s.Range(); it.Good(); it.Next() {
		w.Write([]string{
			it.Time.Format(time.RFC3339Nano),
			strconv.FormatInt(it.Value, 10),
		})
	}

	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}

	if err := outf.Close(); err != nil {
		log.Fatal(err)
	}
}

func importAction(c *cli.Context) {
	s, err := jikan.NewSeries(c.Args().Get(0))
	if err != nil {
		log.Fatal(err)
	}

	var inf io.ReadCloser

	infile := c.Args().Get(1)
	if infile == "" || infile == "-" {
		inf = os.Stdin
	} else {
		if inf, err = os.OpenFile(infile, os.O_RDONLY, 0644); err != nil {
			log.Fatal(err)
		}
	}

	r := csv.NewReader(inf)

	err = s.Locked(func() error {
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			t, err := time.Parse(time.RFC3339Nano, record[0])
			if err != nil {
				return err
			}

			v, err := strconv.ParseInt(record[1], 10, 64)
			if err != nil {
				return err
			}

			if err := s.Add(t, v); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	app := cli.NewApp()

	app.Name = "jikan"
	app.Usage = "Jikan time-series database tool"
	app.Commands = []cli.Command{
		{
			Name:   "export",
			Usage:  "Export the contents of a database",
			Action: exportAction,
		},
		{
			Name:   "import",
			Usage:  "Import content to a database",
			Action: importAction,
		},
	}

	app.Run(os.Args)
}
