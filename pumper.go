// Copyright 2015 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bdmsg

import (
	"errors"
	"github.com/someonegg/gocontainer/bufpool"
	"github.com/someonegg/gox/syncx"
	"golang.org/x/net/context"
	"sync"
	"sync/atomic"
)

var (
	errUnknownPanic = errors.New("unknown panic")
)

type msgEntry struct {
	t MsgType
	m Msg
}

// Objects implementing the PumperHandler interface can be registered
// to process messages in the message pumper.
type PumperHandler interface {
	Process(ctx context.Context, p *Pumper, t MsgType, m Msg)
}

type PumperStatis struct {
	InTotal    int64
	InProcess  int64
	OutTotal   int64
	OutProcess int64
}

// Pumper represents a message pumper. It has a working loop
// which reads, processes and writes messages continuously.
//
// Multiple goroutines can invoke methods on a Pumper simultaneously.
type Pumper struct {
	err   error
	quitF context.CancelFunc
	stopD syncx.DoneChan

	rw MsgReadWriter
	h  PumperHandler
	ud interface{}

	// read
	rerr error
	rD   syncx.DoneChan
	rQ   chan msgEntry

	// write
	werr error
	wD   syncx.DoneChan
	wQ   chan msgEntry

	stat PumperStatis
}

// NewPumper allocates and returns a new Pumper.
func NewPumper(rw MsgReadWriter, h PumperHandler, inN, outN int) *Pumper {
	t := &Pumper{}
	t.init(rw, h, inN, outN)
	return t
}

func (p *Pumper) init(rw MsgReadWriter, h PumperHandler, inN, outN int) {
	p.stopD = syncx.NewDoneChan()

	p.rw = rw
	p.h = h

	p.rD = syncx.NewDoneChan()
	p.rQ = make(chan msgEntry, inN)
	p.wD = syncx.NewDoneChan()
	p.wQ = make(chan msgEntry, outN)
}

// parent, sn can be nil.
// If sn is not nil, it will be called when the working loop exits.
func (p *Pumper) Start(parent context.Context, sn StopNotifier) {
	if parent == nil {
		parent = context.Background()
	}

	var ctx context.Context
	ctx, p.quitF = context.WithCancel(parent)

	rwctx, rwqF := context.WithCancel(context.Background())
	go p.reading(rwctx)
	go p.writing(rwctx)
	go p.work(ctx, rwqF, sn)
}

func (p *Pumper) work(ctx context.Context,
	rwqF context.CancelFunc, sn StopNotifier) {

	defer p.ending(rwqF, sn)

	for q := false; !q; {
		select {
		case <-ctx.Done():
			q = true
		case e := <-p.rQ:
			atomic.AddInt64(&p.stat.InProcess, 1)
			p.procMsg(ctx, e.t, e.m)
			bufpool.Put(e.m)
		case <-p.rD:
			q = true
		case <-p.wD:
			q = true
		}
	}
}

func (p *Pumper) ending(rwqF context.CancelFunc, sn StopNotifier) {
	if e := recover(); e != nil {
		switch v := e.(type) {
		case error:
			p.err = v
		default:
			p.err = errUnknownPanic
		}
	}

	defer func() { recover() }()
	defer p.stopD.SetDone()

	// if ending from error.
	p.quitF()

	rwqF()
	if sn != nil {
		sn.OnStop()
	}

	<-p.rD
	<-p.wD
}

func (p *Pumper) procMsg(ctx context.Context, t MsgType, m Msg) {
	p.h.Process(ctx, p, t, m)
}

func (p *Pumper) reading(ctx context.Context) {
	defer func() {
		if e := recover(); e != nil {
			switch v := e.(type) {
			case error:
				p.rerr = v
			default:
				p.rerr = errUnknownPanic
			}
		}

		p.rD.SetDone()
	}()

	for q := false; !q; {
		t, m := p.readMsg()

		select {
		case <-ctx.Done():
			q = true
		case p.rQ <- msgEntry{t, m}:
			atomic.AddInt64(&p.stat.InTotal, 1)
		}
	}
}

func (p *Pumper) readMsg() (MsgType, Msg) {
	t, m, err := p.rw.ReadMsg()
	if err != nil {
		panic(err)
	}
	return t, m
}

func (p *Pumper) writing(ctx context.Context) {
	defer func() {
		if e := recover(); e != nil {
			switch v := e.(type) {
			case error:
				p.werr = v
			default:
				p.werr = errUnknownPanic
			}
		}

		p.wD.SetDone()
	}()

	for q := false; !q; {
		select {
		case <-ctx.Done():
			q = true
		case e := <-p.wQ:
			atomic.AddInt64(&p.stat.OutProcess, 1)
			p.writeMsg(e.t, e.m)
			bufpool.Put(e.m)
		}
	}
}

func (p *Pumper) writeMsg(t MsgType, m Msg) {
	err := p.rw.WriteMsg(t, m)
	if err != nil {
		panic(err)
	}
}

// Return non-nil if an error has happened.
// When errored, the pumper will stop.
func (p *Pumper) Err() error {
	if p.err != nil {
		return p.err
	}
	if p.rerr != nil {
		return p.rerr
	}
	return p.werr
}

// Request to stop the working loop.
func (p *Pumper) Stop() {
	p.quitF()
}

// Returns a done channel, it will be
// signaled when the pumper is stopped.
func (p *Pumper) StopD() syncx.DoneChanR {
	return p.stopD.R()
}

func (p *Pumper) Stopped() bool {
	return p.stopD.R().Done()
}

// Copy the message data to the in-queue.
func (p *Pumper) Input(t MsgType, m Msg) {
	cp := bufpool.Get(len(m))
	copy(cp, m)
	select {
	case p.rQ <- msgEntry{t, cp}:
		atomic.AddInt64(&p.stat.InTotal, 1)
	case <-p.stopD:
		bufpool.Put(cp)
	}
}

// Try copy the message data to the in-queue.
func (p *Pumper) TryInput(t MsgType, m Msg) bool {
	cp := bufpool.Get(len(m))
	copy(cp, m)
	select {
	case p.rQ <- msgEntry{t, cp}:
		atomic.AddInt64(&p.stat.InTotal, 1)
		return true
	default:
		bufpool.Put(cp)
		return false
	}
}

// Copy the message data to the out-queue.
func (p *Pumper) Output(t MsgType, m Msg) {
	cp := bufpool.Get(len(m))
	copy(cp, m)
	select {
	case p.wQ <- msgEntry{t, cp}:
		atomic.AddInt64(&p.stat.OutTotal, 1)
	case <-p.stopD:
		bufpool.Put(cp)
	}
}

// Try copy the message data to the out-queue.
func (p *Pumper) TryOutput(t MsgType, m Msg) bool {
	cp := bufpool.Get(len(m))
	copy(cp, m)
	select {
	case p.wQ <- msgEntry{t, cp}:
		atomic.AddInt64(&p.stat.OutTotal, 1)
		return true
	default:
		bufpool.Put(cp)
		return false
	}
}

func (p *Pumper) Statis() *PumperStatis {
	return &PumperStatis{
		InTotal:    atomic.LoadInt64(&p.stat.InTotal),
		InProcess:  atomic.LoadInt64(&p.stat.InProcess),
		OutTotal:   atomic.LoadInt64(&p.stat.OutTotal),
		OutProcess: atomic.LoadInt64(&p.stat.OutProcess),
	}
}

// Return the data user setted.
func (p *Pumper) UserData() interface{} {
	return p.ud
}

func (p *Pumper) SetUserData(ud interface{}) {
	p.ud = ud
}

// Return the inner message readwriter.
// You should make sure that its implements support concurrently
// access if you want to call its methods.
func (p *Pumper) InnerMsgRW() MsgReadWriter {
	return p.rw
}

// PumpMux is an message request multiplexer.
//
// It matches the type of each message against a list of registered
// types and calls the matched handler.
//
// Multiple goroutines can invoke methods on a PumpMux simultaneously.
type PumpMux struct {
	mu     sync.RWMutex
	m      map[MsgType]PumperHandler
	orphan PumperHandler
}

// NewPumpMux allocates and returns a new PumpMux.
func NewPumpMux(orphan PumperHandler) *PumpMux {
	return &PumpMux{
		m:      make(map[MsgType]PumperHandler),
		orphan: orphan,
	}
}

// Process dispatches the request to the handler whose
// type matches the message type.
func (mux *PumpMux) Process(ctx context.Context, p *Pumper, t MsgType, m Msg) {
	mux.mu.RLock()
	defer mux.mu.RUnlock()

	if h, ok := mux.m[t]; ok {
		h.Process(ctx, p, t, m)
	} else {
		if mux.orphan != nil {
			mux.orphan.Process(ctx, p, t, m)
		}
	}
}

// Handle registers the handler for the given type.
// If a handler already exists for type, Handle panics.
func (mux *PumpMux) Handle(t MsgType, h PumperHandler) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if h == nil {
		panic("msgpump: nil handler")
	}
	if _, ok := mux.m[t]; ok {
		panic("msgpump: multiple registrations")
	}

	mux.m[t] = h
}

// HandleFunc registers the handler function for the given type.
func (mux *PumpMux) HandleFunc(t MsgType, h func(context.Context,
	*Pumper, MsgType, Msg)) {

	mux.Handle(t, HandlerFunc(h))
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as message handlers.  If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// PumperHandler object that calls f.
type HandlerFunc func(context.Context, *Pumper, MsgType, Msg)

// Process calls f(ctx, p, t, m).
func (f HandlerFunc) Process(ctx context.Context, p *Pumper, t MsgType, m Msg) {
	f(ctx, p, t, m)
}
