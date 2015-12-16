// Copyright 2015 someonegg. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bgmsg implements bidirectional directly message protocol.
//
// Here, message is defined as variable-length byte array, they are
// distinguished by message-type, they can be freely transferred between
// the server and the client.
package bdmsg
