// Copyright 2015 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bdmsg

import (
	"errors"
	"github.com/someonegg/gocontainer/list"
	"github.com/someonegg/gocontainer/queue"
	"github.com/someonegg/gocontainer/rbuf"
	"github.com/someonegg/goutility/chanutil"
	"github.com/someonegg/goutility/poolutil"
	"golang.org/x/net/context"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errUnknownPanic = errors.New("unknown panic")
)

type msgEntry struct {
	list.DefElem
	id MsgId
	m  Msg
}

var msgeFree = sync.Pool{
	New: func() interface{} { return new(msgEntry) },
}

func newMsgEntry(id MsgId, m Msg) *msgEntry {
	e := msgeFree.Get().(*msgEntry)
	e.id = id
	e.m = m
	return e
}

func freeMsgEntry(e *msgEntry) {
	e.id = 0
	e.m = nil
	msgeFree.Put(e)
}

// Objects implementing the Handler interface can be registered
// to process messages in the message pumper.
type Handler interface {
	Process(ctx context.Context, p *Pumper, id MsgId, m Msg)
}

type PumpStat struct {
	InN      int64
	PauseN   int64
	ProcessN int64
	OutN     int64
	BackupN  int64
}

// Pumper represents a message pumper. It has a working loop
// which reads, processes and writes messages continuously.
//
// Multiple goroutines can invoke methods on a Pumper simultaneously.
type Pumper struct {
	err   error
	quitF context.CancelFunc
	stopD chanutil.DoneChan

	rw MsgReadWriter

	// read
	rerr error
	rsD  chanutil.DoneChan
	rQ   queue.EvtQueue
	imax int

	// write
	werr error
	wsD  chanutil.DoneChan
	wQ   queue.EvtQueue

	stat PumpStat

	// backup
	bI msgInfoRing
	bB rbuf.RingBuf

	newer     *Pumper
	newerLock sync.RWMutex

	h Handler

	ud interface{}
}

// NewPumper allocates and returns a new Pumper.
func NewPumper(rw MsgReadWriter, h Handler,
	ud interface{}, maxIn, maxBackup int) *Pumper {

	t := &Pumper{}
	t.init(rw, h, ud, maxIn, maxBackup)
	return t
}

func (p *Pumper) init(rw MsgReadWriter, h Handler,
	ud interface{}, maxIn, maxBackup int) {

	p.stopD = chanutil.NewDoneChan()
	p.rw = rw
	p.rsD = chanutil.NewDoneChan()
	p.imax = maxIn
	p.wsD = chanutil.NewDoneChan()
	p.h = h
	p.ud = ud

	p.rQ.Init()
	p.wQ.Init()

	p.bI.N = 0
	if maxBackup > 0 {
		p.bI.N = maxBackup
		p.bI.Init()
		p.bB.N = 1024
		p.bB.GrowthUnit = 1024
		p.bB.Init()
	}
}

type StopNotifier interface {
	OnStop()
}

// parent, sn can be nil.
// If sn is not nil, it will be called when the working loop exits.
func (p *Pumper) Start(parent context.Context, sn StopNotifier) {
	if parent == nil {
		parent = context.Background()
	}

	var ctx context.Context
	ctx, p.quitF = context.WithCancel(parent)

	var rwctx context.Context
	var rwqF context.CancelFunc
	rwctx, rwqF = context.WithCancel(context.Background())
	go p.work(ctx, rwqF, sn)
	go p.bgRead(rwctx)
	go p.bgWrite(rwctx)
}

func (p *Pumper) work(ctx context.Context,
	rwqF context.CancelFunc, sn StopNotifier) {

	defer p.ending(rwqF, sn)

	rEvt := p.rQ.Event()

	for q := false; !q; {
		select {
		case <-ctx.Done():
			q = true
		case <-rEvt:
			e := p.popRMsg()
			if e != nil {
				atomic.AddInt64(&p.stat.ProcessN, 1)
				p.procMsg(ctx, e.id, e.m)
				poolutil.BufPut(e.m)
				freeMsgEntry(e)
			}
		case <-p.rsD:
			q = true
		case <-p.wsD:
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

	// if ending from error.
	p.Stop()

	rwqF()
	if sn != nil {
		sn.OnStop()
	}

	<-p.rsD
	<-p.wsD

	p.stopD.SetDone()
}

func (p *Pumper) procMsg(ctx context.Context, id MsgId, m Msg) {
	p.h.Process(ctx, p, id, m)
}

const readPauseTime = 10 * time.Millisecond

func (p *Pumper) bgRead(ctx context.Context) {
	defer p.brEnding()

	for q := false; !q; {
		id, m := p.readMsg()
		p.pushRMsg(id, m)
		atomic.AddInt64(&p.stat.InN, 1)

		if p.imax > 0 && p.rQ.Len() > p.imax {

			for q := false; !q && p.rQ.Len() > p.imax; {
				atomic.AddInt64(&p.stat.PauseN, 1)
				select {
				case <-ctx.Done():
					q = true
				case <-time.After(readPauseTime):
				}
			}
		}

		select {
		case <-ctx.Done():
			q = true
		default:
		}
	}
}

func (p *Pumper) brEnding() {
	if e := recover(); e != nil {
		switch v := e.(type) {
		case error:
			p.rerr = v
		default:
			p.rerr = errUnknownPanic
		}
	}

	p.rsD.SetDone()
}

func (p *Pumper) pushRMsg(id MsgId, m Msg) {
	p.rQ.PushBack(newMsgEntry(id, m))
}

func (p *Pumper) popRMsg() *msgEntry {
	e := p.rQ.PopFront()
	if e == nil {
		return nil
	}
	return e.(*msgEntry)
}

func (p *Pumper) readMsg() (MsgId, Msg) {
	id, m, err := p.rw.ReadMsg()
	if err != nil {
		panic(err)
	}
	return id, m
}

func (p *Pumper) bgWrite(ctx context.Context) {
	defer p.bwEnding()

	wEvt := p.wQ.Event()

	for q := false; !q; {
		select {
		case <-ctx.Done():
			q = true
		case <-wEvt:
			e := p.popWMsg()
			if e != nil {
				p.backupWMsg(e.id, e.m)
				atomic.AddInt64(&p.stat.OutN, 1)
				p.writeMsg(e.id, e.m)
				poolutil.BufPut(e.m)
				freeMsgEntry(e)
			}
		}
	}
}

func (p *Pumper) bwEnding() {
	if e := recover(); e != nil {
		switch v := e.(type) {
		case error:
			p.werr = v
		default:
			p.werr = errUnknownPanic
		}
	}

	p.wsD.SetDone()
}

func (p *Pumper) pushWMsg(id MsgId, m Msg) {
	p.wQ.PushBack(newMsgEntry(id, m))
}

func (p *Pumper) popWMsg() *msgEntry {
	e := p.wQ.PopFront()
	if e == nil {
		return nil
	}
	return e.(*msgEntry)
}

func (p *Pumper) writeMsg(id MsgId, m Msg) {
	err := p.rw.WriteMsg(id, m)
	if err != nil {
		panic(err)
	}
}

func (p *Pumper) backupWMsg(id MsgId, m Msg) {
	if p.bI.N <= 0 {
		return
	}

	if p.bI.Full() {
		// remove oldest
		_, l := p.bI.PopFront()
		p.bB.Skip(l)
	}

	p.bI.PushBack(id, len(m))
	p.bB.Write(m)

	atomic.StoreInt64(&p.stat.BackupN, int64(p.bI.Len()))
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
func (p *Pumper) StopD() chanutil.DoneChanR {
	return p.stopD.R()
}

func (p *Pumper) Stopped() bool {
	return p.stopD.R().Done()
}

// Copy the message data to the in-queue.
func (p *Pumper) PostIn(id MsgId, m Msg) {
	p.newerLock.RLock()
	defer p.newerLock.RUnlock()
	if p.newer != nil {
		p.newer.PostIn(id, m)
	} else {
		cp := poolutil.BufGet(len(m))
		copy(cp, m)
		p.pushRMsg(id, cp)
	}
}

// Copy the message data to the out-queue.
func (p *Pumper) PostOut(id MsgId, m Msg) {
	p.newerLock.RLock()
	defer p.newerLock.RUnlock()
	if p.newer != nil {
		p.newer.PostOut(id, m)
	} else {
		cp := poolutil.BufGet(len(m))
		copy(cp, m)
		p.pushWMsg(id, cp)
	}
}

func (p *Pumper) PumpStat() *PumpStat {
	return &PumpStat{
		InN:      atomic.LoadInt64(&p.stat.InN),
		PauseN:   atomic.LoadInt64(&p.stat.PauseN),
		ProcessN: atomic.LoadInt64(&p.stat.ProcessN),
		OutN:     atomic.LoadInt64(&p.stat.OutN),
		BackupN:  atomic.LoadInt64(&p.stat.BackupN),
	}
}

// Only io error is inheritable.
// The pumper must have stopped.
func (p *Pumper) Inheritable() bool {
	return (p.err == nil)
}

// Inherit inner data from the old pumper.
// The old pumper must have stopped.
func (p *Pumper) Inherit(older *Pumper, reOutN int) bool {
	op := older
	op.newerLock.Lock()
	defer op.newerLock.Unlock()

	if reOutN > op.bI.Len() {
		return false
	}

	op.rQ.Lock()
	defer op.rQ.Unlock()
	op.wQ.Lock()
	defer op.wQ.Unlock()

	p.rQ.Lock()
	defer p.rQ.Unlock()
	p.wQ.Lock()
	defer p.wQ.Unlock()

	// in
	p.rQ.List().MergeFrontList(op.rQ.List())

	// backup
	useless := op.bI.Len() - reOutN
	for i := 0; i < useless; i++ {
		_, l := op.bI.PopFront()
		op.bB.Skip(l)
	}
	for i := 0; i < reOutN; i++ {
		id, l := op.bI.PopFront()
		m := poolutil.BufGet(l)
		op.bB.Read(m)
		p.wQ.List().PushBack(newMsgEntry(id, m))
	}

	// out
	p.wQ.List().MergeBackList(op.wQ.List())

	op.newer = p

	p.rQ.SetEvent()
	p.wQ.SetEvent()

	return true
}

// Return the data user setted.
func (p *Pumper) UserData() interface{} {
	return p.ud
}

// Return the inner message readwriter.
// You should make sure that its implements support concurrently
// access if you want to call its methods.
func (p *Pumper) InnerMsgRW() MsgReadWriter {
	return p.rw
}

// PumpMux is an message request multiplexer.
//
// It matches the id of each message against a list of registered
// ids and calls the handler for the id that matches.
//
// Multiple goroutines can invoke methods on a PumpMux simultaneously.
type PumpMux struct {
	mu     sync.RWMutex
	m      map[MsgId]Handler
	orphan Handler
}

// NewPumpMux allocates and returns a new PumpMux.
func NewPumpMux(orphan Handler) *PumpMux {
	return &PumpMux{
		m:      make(map[MsgId]Handler),
		orphan: orphan,
	}
}

// Process dispatches the request to the handler whose
// id matches the message id.
func (mux *PumpMux) Process(ctx context.Context, p *Pumper, id MsgId, m Msg) {
	mux.mu.RLock()
	defer mux.mu.RUnlock()

	if h, ok := mux.m[id]; ok {
		h.Process(ctx, p, id, m)
	} else {
		if mux.orphan != nil {
			mux.orphan.Process(ctx, p, id, m)
		}
	}
}

// Handle registers the handler for the given id.
// If a handler already exists for id, Handle panics.
func (mux *PumpMux) Handle(id MsgId, h Handler) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if h == nil {
		panic("msgpump: nil handler")
	}
	if _, ok := mux.m[id]; ok {
		panic("msgpump: multiple registrations")
	}

	mux.m[id] = h
}

// HandleFunc registers the handler function for the given id.
func (mux *PumpMux) HandleFunc(id MsgId, h func(context.Context,
	*Pumper, MsgId, Msg)) {

	mux.Handle(id, HandlerFunc(h))
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as message handlers.  If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler object that calls f.
type HandlerFunc func(context.Context, *Pumper, MsgId, Msg)

// Process calls f(ctx, p, id, m).
func (f HandlerFunc) Process(ctx context.Context, p *Pumper, id MsgId, m Msg) {
	f(ctx, p, id, m)
}
