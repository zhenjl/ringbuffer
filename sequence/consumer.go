// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

package sequence

import (
	"github.com/reducedb/ringbuffer"
	"log"
	"math"
	"runtime"
)

var _ = log.Ldate

type Consumer struct {
	sequencer
}

var _ ringbuffer.Sequencer = (*Consumer)(nil)

func NewConsumer(bufferSize int) (ringbuffer.Sequencer, error) {
	if !ringbuffer.PowerOfTwo(int(bufferSize)) {
		return nil, ErrNotPowerOfTwo
	}

	s := &Consumer{}
	s.cursor = InitialSequenceValue
	s.cachedGate = InitialSequenceValue
	s.bufferSize = bufferSize

	return s, nil
}

func (this *Consumer) Next(n int) (int64, error) {
	nextSeq, err := this.Request(n)
	if err != nil {
		return 0, err
	}

	this.Commit(nextSeq)

	return nextSeq, nil
}

func (this *Consumer) Request(n int) (int64, error) {
	if n < 1 {
		return 0, ErrNotPositiveInteger
	}

	next, err := this.Get()
	if err != nil {
		return 0, err
	}

	nextSeq := next + int64(n)
	cachedGate := this.cachedGate

	if nextSeq > cachedGate {
		var minSeq int64
		var err error

		for minSeq, err = ringbuffer.GetMinSeq(this.gates, math.MaxInt64); nextSeq > minSeq; minSeq, err = ringbuffer.GetMinSeq(this.gates, math.MaxInt64) {
			//log.Printf("consumer nextSeq = %d, minSeq = %d\n", nextSeq, minSeq)
			if err != nil {
				return 0, err
			}
			runtime.Gosched()
		}

		this.cachedGate = minSeq
	}

	return nextSeq, nil
}
