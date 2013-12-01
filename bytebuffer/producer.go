// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

package bytebuffer

import (
	"github.com/reducedb/ringbuffer"
	"github.com/reducedb/ringbuffer/sequence"
	"log"
)

var _ = log.Ldate

type producer struct {
	buffer *byteBuffer
	seq    ringbuffer.Sequencer
}

var _ ringbuffer.Producer = (*producer)(nil)

func (this *byteBuffer) NewProducer() (ringbuffer.Producer, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if len(this.producers) < MaxProducerCount {
		seq, err := sequence.NewProducer(this.SlotCount())
		if err != nil {
			return nil, err
		}

		p := &producer{
			buffer: this,
			seq:    seq,
		}

		this.producers = append(this.producers, p)

		for _, c := range this.consumers {
			c.seq.AddGatingSequence(p.seq)
			p.seq.AddGatingSequence(c.seq)
		}

		return p, nil

	}

	return nil, ErrMaxProducerCountExceeded
}

// Put writes the data to ring buffer.
// Returns the number of elements written. The unit of the element depends on
// the storage engine used.
func (this *producer) Put(data interface{}) (int, error) {
	src, err := this.buffer.validData(data)
	if err != nil {
		return 0, err
	}

	needed, err := this.buffer.SlotsNeeded(len(src))
	//log.Printf("slots needed = %d\n", needed)
	if err != nil {
		return 0, err
	}

	//log.Printf("slots needed = %d\n", needed)

	seq, err := this.seq.Request(needed)
	if err != nil {
		return 0, err
	}

	//log.Printf("slots needed = %d, seq = %d, data = %#v\n", needed, seq, src)

	n, err := this.buffer.Put(src, seq+1-int64(needed))
	if err != nil {
		return 0, err
	}

	//log.Printf("Producer: commit %d\n", seq)
	this.seq.Commit(seq)

	return n, nil
}
