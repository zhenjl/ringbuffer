// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

package sequence

import (
	"log"
	"github.com/reducedb/ringbuffer"
)

var _ = log.Ldate

type Producer struct {
	sequencer
}

var _ ringbuffer.Sequencer = (*Producer)(nil)

func NewProducer(bufferSize int) (ringbuffer.Sequencer, error) {
	if !ringbuffer.PowerOfTwo(int(bufferSize)) {
		return nil, ErrNotPowerOfTwo
	}

	s := &Producer{}
	s.cursor = InitialSequenceValue
	s.cachedGate = InitialSequenceValue
	s.bufferSize = bufferSize

	return s, nil
}

func (this *Producer) Next(n int) (int64, error) {
	nextSeq, err := this.Request(n)
	if err != nil {
		return 0, err
	}

	this.Commit(nextSeq)

	return nextSeq, nil
}

func (this *Producer) Request(n int) (int64, error) {
	if n < 1 {
		return 0, ErrNotPositiveInteger
	}

	cursor, err := this.Get()
	if err != nil {
		return 0, err
	}

	nextSeq := cursor + int64(n)
	wrapPoint := nextSeq - int64(this.bufferSize)
	cachedGate := this.cachedGate

	//log.Printf("producer cursor = %d, nextSeq = %d, wrapPoint = %d, cachedGate = %d\n", cursor, nextSeq, wrapPoint, cachedGate)

	// For the producer, cachedGate is the previous lowest consumer sequence. If wrapPoint
	// is greater than cacehdGate, that means the consumer has't reached the next slot yet,
	// which means we cannot do anything// with that slot until all the consumers have
	// passed it. So we wait...
	//
	// TODO: Figure out what "cachedGate > next" means
	if wrapPoint > cachedGate || cachedGate > cursor {
		var minSeq int64
		var err error

		for minSeq, err = ringbuffer.GetMinSeq(this.gates, cursor); wrapPoint > minSeq; minSeq, err = ringbuffer.GetMinSeq(this.gates, cursor) {
			if err != nil {
				return 0, err
			}
			//runtime.Gosched()
		}

		this.cachedGate = minSeq
	}

	return nextSeq, nil
}