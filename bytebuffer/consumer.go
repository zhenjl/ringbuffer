// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

package bytebuffer

import (
	"github.com/reducedb/ringbuffer"
	"github.com/reducedb/ringbuffer/sequence"
	"log"
)

var _ = log.Ldate

type consumer struct {
	buffer *byteBuffer
	seq    ringbuffer.Sequencer
}

var _ ringbuffer.Consumer = (*consumer)(nil)

func (this *byteBuffer) NewConsumer() (ringbuffer.Consumer, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	seq, err := sequence.NewConsumer(this.SlotCount())
	if err != nil {
		return nil, err
	}

	c := &consumer{
		buffer: this,
		seq:    seq,
	}

	this.consumers = append(this.consumers, c)

	for _, p := range this.producers {
		p.seq.AddGatingSequence(c.seq)
		c.seq.AddGatingSequence(p.seq)
	}

	return c, nil
}

func (this *consumer) Get() (interface{}, error) {
	seq, err := this.seq.Request(1)
	if err != nil {
		return 0, err
	}

	size := this.buffer.NextDataSize(seq)
	needed, err := this.buffer.SlotsNeeded(size)
	if err != nil {
		return 0, err
	}

	seq, err = this.seq.Request(needed)
	if err != nil {
		return 0, err
	}
	//log.Printf("consumer: size = %d, needed = %d, seq = %d\n", size, needed, seq)

	data, err := this.buffer.Get(seq + 1 - int64(needed))
	if err != nil {
		return 0, err
	}

	//log.Printf("consumer: commit %d\n", seq)
	this.seq.Commit(seq)

	return data, nil
}
