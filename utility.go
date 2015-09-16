// Copyright 2015 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bdmsg

import (
	"errors"
)

var (
	errRingFull  = errors.New("ring is full")
	errRingEmpty = errors.New("ring is empty")
)

type msgInfo struct {
	id   MsgId
	size int
}

type msgInfoRing struct {
	// capacity
	N int

	infos []msgInfo
	beg   int
	size  int
}

func (r *msgInfoRing) Init() {
	r.infos = make([]msgInfo, r.N, r.N)
	r.beg = 0
	r.size = 0
}

func (r *msgInfoRing) Len() int {
	return r.size
}

func (r *msgInfoRing) Full() bool {
	return r.size >= r.N
}

// Panic if full.
func (r *msgInfoRing) PushBack(id MsgId, size int) {
	if r.size >= r.N {
		panic(errRingFull)
	}
	i := (r.beg + r.size) % r.N
	r.infos[i].id = id
	r.infos[i].size = size
	r.size += 1
}

// Panic if empty.
func (r *msgInfoRing) PopFront() (MsgId, int) {
	if r.size == 0 {
		panic(errRingEmpty)
	}
	info := r.infos[r.beg]
	r.size -= 1
	r.beg = (r.beg + 1) % r.N
	return info.id, info.size
}
