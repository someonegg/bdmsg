// Copyright 2015 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bdmsg implements bidirectional directly message protocol.
// Message is defined as variable-length byte array, they are
// distinguished by message-type, they can be freely transferred between
// the server and the client.
//
// You can provide your own "Converter" to customize the message IO.
// The default converter "DefaultIOC" use "MsgRWIO", message layout is:
//  A(length) + B(type) + C(data)
//  A is 4-bytes int, big-endian
//  B is 4-bytes int, big-endian
//  C is byte array, its length is (A) - 4
//
// Here is a quick example, includes protocol, server, client.
//
// Protocol
//  const (
//      MsgTypeConnect      = 1
//      MsgTypeConnectReply = 2
//  )
//
//  type ConnectRequst struct {
//      Name string
//      Pass string
//  }
//
//  func (p *ConnectRequst) Marshal() ([]byte, error) {
//      return json.Marshal(p)
//  }
//
//  func (p *ConnectRequst) Unmarshal(b []byte) error {
//      return json.Unmarshal(b, p)
//  }
//
//  type ConnectReply struct {
//      Code  int
//      Token string
//  }
//
//  func (p *ConnectReply) Marshal() ([]byte, error) {
//      return json.Marshal(p)
//  }
//
//  func (p *ConnectReply) Unmarshal(b []byte) error {
//      return json.Unmarshal(b, p)
//  }
//
// Server
//  type server struct {
//      *bdmsg.Server
//  }
//
//  func newService(l net.Listener, handshakeTO time.Duration,
//      pumperInN, pumperOutN int) *server {
//
//      s := &server{}
//
//      mux := bdmsg.NewPumpMux(nil)
//      mux.HandleFunc(MsgTypeConnect, s.handleConnect)
//
//      s.Server = bdmsg.NewServerF(l, bdmsg.DefaultIOC, handshakeTO,
//          mux, pumperInN, pumperOutN)
//      return s
//  }
//
//  func (s *server) handleConnect(ctx context.Context,
//      p *bdmsg.Pumper, t bdmsg.MsgType, m bdmsg.Msg) {
//
//      msc := p.UserData().(*bdmsg.SClient)
//      if msc.Handshaked() {
//          panic(errors.New("Unexpected"))
//      }
//
//      var request ConnectRequst
//      request.Unmarshal(m) // unmarshal request
//
//      // process connect request
//
//      // tell bdmsg that client is authorized
//      msc.Handshake()
//
//      var reply ConnectReply
//      // init reply
//      mr, _ := reply.Marshal() // marshal reply
//
//      msc.Output(MsgTypeConnectReply, mr)
//  }
// Client
//  type client struct {
//      *bdmsg.Client
//      connected bool
//  }
//
//  func newClient(conn net.Conn, pumperInN, pumperOutN int) *client {
//
//      c := &client{}
//
//      mux := bdmsg.NewPumpMux(nil)
//      mux.HandleFunc(MsgTypeConnectReply, c.handleConnectReply)
//
//      c.Client = bdmsg.NewClient(nil, conn, bdmsg.DefaultIOC,
//          mux, pumperInN, pumperOutN)
//      c.doConnect()
//      return c
//  }
//
//  func (c *client) doConnect() {
//      var request ConnectRequst
//      // init request
//      mr, _ := request.Marshal() // marshal request
//
//      c.Client.Output(MsgTypeConnect, mr)
//  }
//
//  func (c *client) handleConnectReply(ctx context.Context,
//      p *bdmsg.Pumper, t bdmsg.MsgType, m bdmsg.Msg) {
//
//      var reply ConnectReply
//      reply.Unmarshal(m) // unmarshal reply
//
//      // process connect reply
//
//      c.connected = true
//  }
package bdmsg
