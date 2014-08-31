Jikan
=====

Time-series database written in Go

Overview
--------

Jikan is a time-series database implemented in the Go programming language. It
implements concepts outlined in [Specialized Storage for Big Numeric Time
Series](https://www.usenix.org/conference/hotstorage13/workshop-program/presentation/shafer)
to achieve a very compact on-disk representation, at the expense of efficiency
in areas that the author deemed unimportant.

Jikan is specifically designed to store very long runs of time/integer pairs. In
the representation that it uses, times and values are stored as deltas against
the previous entry. They are also stored as variable-width integers, allowing a
small change in value to translate to a small entry on-disk. In the future, it's
planned that Jikan will support run-length encoding on these deltas and values,
enabling significant space savings, even compared to its current strategy.

Jikan is also designed first and foremost with a very modular design, allowing
it to be embedded easily into other applications. You can see the API
documentation here:

[http://godoc.org/oakwilson.com/p/jikan/core](http://godoc.org/oakwilson.com/p/jikan/core)

Installation
------------

Pretty simple!

```
$ go get oakwilson.com/p/jikan && go install oakwilson.com/p/jikan
```

CLI Usage
---------

```
NAME:
   jikan - Jikan time-series database tool

USAGE:
   jikan [global options] command [command options] [arguments...]

VERSION:
   0.0.0

COMMANDS:
   export, e  Export the contents of a database
   import, i  Import content to a database
   help, h    Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --debug, -d    enable debug logging
   --help, -h     show help
   --version, -v  print the version
```

License
-------

3-clause BSD. A copy is included with the source.
