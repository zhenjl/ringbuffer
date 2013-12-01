// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

package sequence

import (
	"sync/atomic"
)

const (
	InitialSequenceValue = -1
)

// To avoid false sharing, e.g., having the same sequence variables in the same cache line,
// we will make each sequence 64 bytes, which is the most common cache line size.
type sequence struct {
	cursor, cachedGate, p2, p3, p4, p5, p6, p7 int64
}

func NewSequence() *sequence {
	return &sequence{
		cursor:     InitialSequenceValue,
		cachedGate: InitialSequenceValue,
	}
}

func (this *sequence) Get() (int64, error) {
	return atomic.LoadInt64(&this.cursor), nil
}

func (this *sequence) Set(seq int64) error {
	atomic.StoreInt64(&this.cursor, seq)
	return nil
}
