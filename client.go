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

// NewClient allocates and returns a new Client.
//
// The ownership of c will be transferred to Client, dont
// control it in other places.
func NewClient(parent context.Context, c net.Conn, ioc Converter,
	h PumperHandler, pumperInN, pumperOutN int) *Client {

	rw := ioc.Convert(c)

	t := &Client{}
	t.c = c
	t.Pumper.init(rw, h, pumperInN, pumperOutN)
	t.Pumper.SetUserData(t)
	t.Pumper.Start(parent, t)

	return t
}

func (c *Client) OnStop() {
	c.c.Close()
	if sn, ok := c.rw.(StopNotifier); ok {
		sn.OnStop()
	}
}

// The pumper's initial userdata is *Client.
func (c *Client) InnerPumper() *Pumper {
	return &c.Pumper
}

func (c *Client) Conn() net.Conn {
	return c.c
}
