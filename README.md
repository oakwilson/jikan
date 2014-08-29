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
small change in value to translate to a small entry on-disk.

Installation
------------

Pretty simple!

```
$ go install oakwilson.com/p/jikan
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
   export Export the contents of a database
   import Import content to a database
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h   show help
   --version, -v  print the version
```

License
-------

3-clause BSD. A copy is included with the source.
