// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

package bytebuffer

import (
	"encoding/binary"
	"fmt"
	"github.com/reducedb/ringbuffer"
	"log"
	"math"
	"sync"
)

var _ = log.Ldate

const (
	// Number of overhead bytes for each data item
	SlotOverhead = 2

	// Minimum slot size
	MinSlotSize = SlotOverhead

	// Maximum data size, -SlotOverhead because we are using 2 bytes to store the size of the data
	MaxDataSize = 64*1024 - SlotOverhead

	// Maximum number of slots the data item can occupy
	MaxDataSlots = MaxDataSize / MinSlotSize

	MaxProducerCount = 1
)

var (
	ErrDataInvalid              = fmt.Errorf("bytebuffer: Data Invalid")
	ErrDataExceedsMaxSize       = fmt.Errorf("bytebuffer: Data Size Exceeds MaxDataSize")
	ErrDataExceedsMaxSlots      = fmt.Errorf("bytebuffer: Data Size Exceeds MaxDataSlots")
	ErrSlotSizeTooSmall         = fmt.Errorf("bytebuffer: Slot Size Is Too Small")
	ErrNotPowerOfTwo            = fmt.Errorf("bytebuffer: Slot Count Must Be Power of Two")
	ErrMaxProducerCountExceeded = fmt.Errorf("bytebuffer: This ringbuffer only allows %d producer(s)", MaxProducerCount)
	ErrMaxDataSlotsExceeded     = fmt.Errorf("bytebuffer: Max Data Slots (%d) Exceeded", MaxDataSlots)
)

//
type byteBuffer struct {
	buffer []byte

	slotSize   int
	slotCount  int
	slotMask   int
	bufferSize int64

	tmpSize [2]byte
	tmpbuf  []byte

	producers []*producer
	consumers []*consumer
	mutex     sync.RWMutex
}

var _ ringbuffer.RingBuffer = (*byteBuffer)(nil)

func New(slotSize, slotCount int) (ringbuffer.RingBuffer, error) {
	if slotSize < MinSlotSize {
		return nil, ErrSlotSizeTooSmall
	}

	if slotCount > MaxDataSlots {
		return nil, ErrMaxDataSlotsExceeded
	}

	if !ringbuffer.PowerOfTwo(slotCount) {
		return nil, ErrNotPowerOfTwo
	}

	slotSize += SlotOverhead

	d := &byteBuffer{
		slotSize:   slotSize,
		slotCount:  slotCount,
		slotMask:   slotCount - 1,
		bufferSize: int64(slotSize * slotCount),
		buffer:     make([]byte, slotSize*slotCount),
		producers:  make([]*producer, 0),
		consumers:  make([]*consumer, 0),
	}

	return d, nil
}

func (this *byteBuffer) Close() error {
	return nil
}

func (this *byteBuffer) Flush() error {
	return nil
}

// Put copies the byte slice into buffer, starting at the slot number calculated from seq.
// If needed, it will wrap around and copy the remaining bytes starting at index 0 of buffer.
//
// The data size cannot exceed the buffer size (thus will cause wrap around from starting
// position), or ErrDataTooLarge is returned.
//
// Data must be []byte or ErrDataInvalid is returned.
func (this *byteBuffer) Put(data []byte, seq int64) (int, error) {
	needed, err := this.SlotsNeeded(len(data))
	if err != nil {
		return 0, err
	}

	n, i, l := len(data), 0, 0

	slot := seq & int64(this.slotMask)
	index := slot * int64(this.slotSize)

	binary.LittleEndian.PutUint16(this.buffer[index:index+SlotOverhead], uint16(n))

	index += SlotOverhead

	for n > 0 {
		l = copy(this.buffer[index:], data[i:])
		i += l
		n -= l

		if n > 0 {
			index = 0
		}
	}

	return needed, nil
}

// Get retrieves a byte slice from the buffer, starting at the slot number calculated from seq.
// If needed, it will wrap around and copy the remaining bytes starting at index 0 of buffer.
//
// The byte slice is almost always a sub-slice of the main buffer, so the caller should NOT be
// modifying the slice in place. This is done to avoid unnecessary copying of data. However,
// if the data was wrapped in the ring buffer (part at the end, part at the beginning), then
// a new []byte is allocated to hold the data. To the caller, it shouldn't matter however.
func (this *byteBuffer) Get(seq int64) ([]byte, error) {
	slot := seq & int64(this.slotMask)
	index := slot * int64(this.slotSize)

	n := int(binary.LittleEndian.Uint16(this.buffer[index : index+SlotOverhead]))
	if n > MaxDataSize {
		return nil, ErrDataExceedsMaxSize
	}

	index += SlotOverhead

	if index+int64(n) < this.bufferSize {
		return this.buffer[index : index+int64(n)], nil
	}

	if n > len(this.tmpbuf) {
		this.tmpbuf = make([]byte, n)
	}
	l, i := 0, 0

	for n > 0 {
		l = copy(this.tmpbuf[i:], this.buffer[index:])
		n -= l
		i += l

		if n > 0 {
			index = 0
		}
	}

	return this.tmpbuf[:i], nil
}

// SlotSize returns the current slot size. This may be different than what the user originally
// submitted since we have to add SlotOverhead
func (this *byteBuffer) SlotSize() int {
	return this.slotSize
}

// SlotCount return the number of slots in the buffer
func (this *byteBuffer) SlotCount() int {
	return this.slotCount
}

// SlotsNeeded will return the number of slots required for the data supplied
func (this *byteBuffer) SlotsNeeded(size int) (int, error) {
	if size > MaxDataSize {
		return 0, ErrDataExceedsMaxSize
	}

	if size == 0 {
		return 1, nil
	}

	needed := int(math.Ceil(float64(size+SlotOverhead) / float64(this.slotSize)))

	if needed > this.slotCount {
		return 0, ErrDataExceedsMaxSlots
	}

	//log.Printf("needed = %d, slotCount = %d\n", needed, this.slotCount)

	return needed, nil
}

func (this *byteBuffer) NextDataSize(seq int64) int {
	slot := seq & int64(this.slotMask)
	index := slot * int64(this.slotSize)

	return int(binary.LittleEndian.Uint16(this.buffer[index : index+SlotOverhead]))
}

// validData returns whether the data supplied is a valid type for this storage engine
func (this *byteBuffer) validData(data interface{}) ([]byte, error) {
	if v, ok := data.([]byte); ok {
		return v, nil
	}

	return nil, ErrDataInvalid
}
