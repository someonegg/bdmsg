// Copyright 2015 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bdmsg

import (
	"golang.org/x/net/context"
	"net"
)

// Client represents a message client in the client side.
// You can send(recive) message to(from) server use Client.Pumper().
//
// Multiple goroutines can invoke methods on a Client simultaneously.
type Client struct {
	Pumper
	c net.Conn
}

// See NewClientF.
func NewClient(parent context.Context, c net.Conn, h Handler) *Client {
	return NewClientF(parent, c, h, 100, 0, DefaultIOC)
}

// NewClientF allocates and returns a new Client.
//
// The ownership of c will be transferred to Client, dont
// control it in other places.
func NewClientF(parent context.Context, c net.Conn, h Handler,
	pumperMaxIn, pumperMaxBackup int, ioc Converter) *Client {

	rw := ioc.Convert(c)

	t := &Client{}
	t.c = c
	t.Pumper.init(rw, h, t, pumperMaxIn, pumperMaxBackup)
	t.Pumper.Start(parent, t)

	return t
}

func (c *Client) OnStop() {
	c.c.Close()
	if sn, ok := c.rw.(StopNotifier); ok {
		sn.OnStop()
	}
}

// The pumper's userdata is *Client.
func (c *Client) InnerPumper() *Pumper {
	return &c.Pumper
}

func (c *Client) Conn() net.Conn {
	return c.c
}
