package main

import (
	"encoding/csv"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/codegangsta/cli"
	"github.com/demizer/go-elog"
	"oakwilson.com/p/jikan/core"
)

func exportAction(c *cli.Context) {
	db, err := jikan.Open(c.Args().Get(0))
	if err != nil {
		log.Critical(err)
		os.Exit(1)
	}

	s, err := db.Stream([]byte(c.Args().Get(1)))
	if err != nil {
		log.Critical(err)
		os.Exit(1)
	}

	var outf io.WriteCloser

	outfile := c.Args().Get(2)
	if outfile == "" || outfile == "-" {
		outf = os.Stdout
	} else {
		if outf, err = os.OpenFile(outfile, os.O_CREATE|os.O_WRONLY, 0644); err != nil {
			log.Critical(err)
			os.Exit(1)
		}
	}

	w := csv.NewWriter(outf)

	for it := s.Iterator(); it.Good(); it.Next() {
		w.Write([]string{
			it.Time.Format(time.RFC3339Nano),
			strconv.FormatInt(it.Value, 10),
		})
	}

	w.Flush()

	if err := w.Error(); err != nil {
		log.Critical(err)
		os.Exit(1)
	}

	if err := outf.Close(); err != nil {
		log.Critical(err)
		os.Exit(1)
	}

	if err := db.Close(); err != nil {
		log.Critical(err)
		os.Exit(1)
	}
}

func importAction(c *cli.Context) {
	db, err := jikan.Open(c.Args().Get(0))
	if err != nil {
		log.Critical(err)
		os.Exit(1)
	}

	s, err := db.Stream([]byte(c.Args().Get(1)))
	if err != nil {
		log.Critical(err)
		os.Exit(1)
	}

	var inf io.ReadCloser

	infile := c.Args().Get(2)
	if infile == "" || infile == "-" {
		inf = os.Stdin
	} else {
		if inf, err = os.OpenFile(infile, os.O_RDONLY, 0644); err != nil {
			log.Critical(err)
			os.Exit(1)
		}
	}

	r := csv.NewReader(inf)

	err = s.WithTx(func(tx *jikan.StreamTx) error {
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

			if err := tx.Add(t, v); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		log.Critical(err)
		os.Exit(1)
	}

	if err := db.Close(); err != nil {
		log.Critical(err)
		os.Exit(1)
	}
}

func main() {
	log.SetFlags(log.Llabel | log.LshortFileName | log.LlineNumber)

	rand.Seed(time.Now().UnixNano())

	app := cli.NewApp()

	app.Name = "jikan"
	app.Usage = "Jikan time-series database tool"
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "debug",
			Usage: "enable debug logging",
		},
	}
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

	app.Before = func(c *cli.Context) error {
		if c.Bool("debug") {
			log.SetFlags(log.LfunctionName | log.Ltree | log.Llabel)
			log.SetLevel(log.LEVEL_DEBUG)
		}

		return nil
	}

	app.Run(os.Args)
}
