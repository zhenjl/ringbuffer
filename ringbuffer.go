/// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

// Ringbuffer implements a ring buffer that can leverage different storage engines
// and wait strategies. It is modeled after the LMAX Disruptor architecture.
package ringbuffer

import (
	"log"
)

var _ = log.Ldate

type RingBuffer interface {
	NewProducer() (Producer, error)
	NewConsumer() (Consumer, error)
}

type Producer interface {
	Put(interface{}) (int, error)
}

type Consumer interface {
	Get() (interface{}, error)
}

type Sequencer interface {
	Get() (int64, error)
	Set(int64) error
	Next(int) (int64, error)
	Request(int) (int64, error)
	Commit(int64) error
	AddGatingSequence(...Sequencer)
	RemoveGatingSequence(Sequencer)
}

func GetMinSeq(gates []Sequencer, min int64) (int64, error) {
	for _, seq := range gates {		
		if v, err := seq.Get(); err != nil {
			return 0, err
		} else if v < min {
			min = v
		}
	}

	return min, nil
}

// powerOfTwo determines whether n is of power of two
func PowerOfTwo(n int) bool {
	return n != 0 && (n&(n-1)) == 0
}