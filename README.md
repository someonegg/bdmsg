bdmsg
======

bdmsg implements bidirectional directly message protocol with golang.
Message is defined as variable-length byte array, they are
distinguished by message-type, they can be freely transferred between
the server and the client.

Documentation
-------------

- [API Reference](http://godoc.org/github.com/someonegg/bdmsg)

Installation
------------

Install bdmsg using the "go get" command:

    go get github.com/someonegg/bdmsg

The Go distribution is bdmsg's only dependency.
