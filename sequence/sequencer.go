// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

package sequence

import (
	"errors"
	"fmt"
	"github.com/reducedb/ringbuffer"
	"log"
	"sync"
)

var _ = log.Ldate

var (
	ErrNotPositiveInteger = errors.New("sequence.sequencer: Not Positive Integer Value")
	ErrNotPowerOfTwo      = errors.New("ringbuffer.ByteBuffer: Slot Count Must Be Power of Two")
)

type sequencer struct {
	sequence

	gates      []ringbuffer.Sequencer
	gatesMutex sync.RWMutex

	bufferSize int
}

func (this *sequencer) Next(n int) (int64, error) {
	nextSeq, err := this.Request(n)
	if err != nil {
		return 0, err
	}

	this.Commit(nextSeq)

	return nextSeq, nil
}

func (this *sequencer) Request(n int) (int64, error) {
	return 0, fmt.Errorf("Not implemented")
}

func (this *sequencer) Commit(n int64) error {
	return this.Set(n)
}

func (this *sequencer) AddGatingSequence(seq ...ringbuffer.Sequencer) {
	this.gatesMutex.Lock()
	defer this.gatesMutex.Unlock()
	this.gates = append(this.gates, seq...)
}

func (this *sequencer) RemoveGatingSequence(seq ringbuffer.Sequencer) {
	this.gatesMutex.Lock()
	defer this.gatesMutex.Unlock()
	// TODO: add removing sequence routine
}
