// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

package bytebuffer

import (
	"bytes"
	"log"
	"testing"
)

var _ = log.Ldate

func TestErrMaxProducerCountExceeded(t *testing.T) {
	r, err := New(10, 128)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := r.NewProducer(); err != nil {
		t.Fatal(err)
	}

	if _, err := r.NewProducer(); err != ErrMaxProducerCountExceeded {
		t.Fatal("Expecting ErrMaxProducerCountExceeded, didn't get it")
	}
}

func TestProducerDataInvalid(t *testing.T) {
	r, err := New(4, 16)
	if err != nil {
		t.Fatal(err)
	}

	p, err := r.NewProducer()
	if err != nil {
		t.Fatal(err)
	}

	src := make([]int, 100)
	if _, err := p.Put(src); err != ErrDataInvalid {
		t.Fatal("Should have exited with ErrDataInvalid, " + err.Error())
	}
}

func TestProducerNoWrap(t *testing.T) {
	r, err := New(4, 16)
	if err != nil {
		t.Fatal(err)
	}

	p, err := r.NewProducer()
	if err != nil {
		t.Fatal(err)
	}

	data := []byte{1, 2, 3, 4}

	for i := int64(0); i < 10; i++ {
		data[0] = byte(i)
		p.Put(data)
	}

	st := r.(*byteBuffer)

	//log.Printf("%#v\n", st.buffer)

	if !bytes.Equal(st.buffer, []byte{0x4, 0x0, 0x0, 0x2, 0x3, 0x4, 0x4, 0x0, 0x1, 0x2, 0x3, 0x4, 0x4, 0x0, 0x2, 0x2, 0x3, 0x4, 0x4, 0x0, 0x3, 0x2, 0x3, 0x4, 0x4, 0x0, 0x4, 0x2, 0x3, 0x4, 0x4, 0x0, 0x5, 0x2, 0x3, 0x4, 0x4, 0x0, 0x6, 0x2, 0x3, 0x4, 0x4, 0x0, 0x7, 0x2, 0x3, 0x4, 0x4, 0x0, 0x8, 0x2, 0x3, 0x4, 0x4, 0x0, 0x9, 0x2, 0x3, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}) {
		t.Fatalf("bytes not the same")
	}
}

func TestProducerWrap(t *testing.T) {
	r, err := New(4, 16)
	if err != nil {
		t.Fatal(err)
	}

	p, err := r.NewProducer()
	if err != nil {
		t.Fatal(err)
	}

	data := []byte{1, 2, 3, 4, 5, 6, 7}

	for i := int64(0); i < 10; i++ {
		data[0] = byte(i)
		p.Put(data)
	}

	st := r.(*byteBuffer)

	//log.Printf("%#v\n", st.buffer)

	if !bytes.Equal(st.buffer, []byte{0x7, 0x0, 0x8, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x0, 0x0, 0x7, 0x0, 0x9, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x0, 0x0, 0x7, 0x0, 0x2, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x0, 0x0, 0x7, 0x0, 0x3, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x0, 0x0, 0x7, 0x0, 0x4, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x0, 0x0, 0x7, 0x0, 0x5, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x0, 0x0, 0x7, 0x0, 0x6, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x0, 0x0, 0x7, 0x0, 0x7, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x0, 0x0}) {
		t.Fatalf("bytes not the same")
	}
}

func TestProducerVariableSizeNoWrap(t *testing.T) {
	r, err := New(4, 16)
	if err != nil {
		t.Fatal(err)
	}

	p, err := r.NewProducer()
	if err != nil {
		t.Fatal(err)
	}

	data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	for i := int64(0); i < 10; i++ {
		p.Put(data[:i])
	}

	st := r.(*byteBuffer)

	//log.Printf("%#v\n", st.buffer)

	if !bytes.Equal(st.buffer, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x1, 0x2, 0x0, 0x0, 0x3, 0x0, 0x1, 0x2, 0x3, 0x0, 0x4, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x0, 0x0, 0x0, 0x0, 0x7, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x0, 0x0, 0x8, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x0, 0x0, 0x9, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}) {
		t.Fatalf("bytes not the same")
	}
}

func TestProducerVariableSizeWrap(t *testing.T) {
	r, err := New(4, 16)
	if err != nil {
		t.Fatal(err)
	}

	p, err := r.NewProducer()
	if err != nil {
		t.Fatal(err)
	}

	data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	for i := int64(0); i < 12; i++ {
		p.Put(data[:i])
	}

	st := r.(*byteBuffer)

	//log.Printf("%#v\n", st.buffer)

	if !bytes.Equal(st.buffer, []byte{0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0x0, 0x1, 0x2, 0x3, 0x0, 0x4, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x0, 0x0, 0x0, 0x0, 0x7, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x0, 0x0, 0x0, 0x8, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x0, 0x0, 0x9, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0x0, 0xa, 0x0, 0x1, 0x2, 0x3, 0x4}) {
		t.Fatalf("bytes not the same")
	}
}

func TestConsumerNoWrap(t *testing.T) {
	r, err := New(4, 16)
	if err != nil {
		t.Fatal(err)
	}

	p, err := r.NewProducer()
	if err != nil {
		t.Fatal(err)
	}

	c, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}

	data := []byte{1, 2, 3, 4, 5, 6}
	p.Put(data)

	out, err := c.Get()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(out.([]byte), []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6}) {
		t.Fatalf("bytes not the same")
	}
}

func Test1ProducerAnd1Consumer(t *testing.T) {
	r, err := New(4, 16)
	if err != nil {
		t.Fatal(err)
	}

	p, err := r.NewProducer()
	if err != nil {
		t.Fatal(err)
	}

	c, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}

	var count int64 = 16
	data := []byte{1, 2, 3, 4, 5, 6}

	// Producer goroutine
	go func() {
		for i := int64(0); i < count; i++ {
			if _, err := p.Put(data); err != nil {
				t.Fatal(err)
			}
		}
	}()

	var total int64

	for i := int64(0); i < count; i++ {
		if out, err := c.Get(); err != nil {
			t.Fatal(err)
		} else {
			if !bytes.Equal(out.([]byte), data) {
				t.Fatalf("bytes not the same")
			}

			total++
		}
	}

	if total != count {
		t.Fatalf("Expected to have read %d items, got %d\n", count, total)
	}
}

// This test function creates a 128-slot ring buffer, with each slot being 128 bytes long.
// It also creates 1 producer and 1 consumer, where the producer will put the same byte
// slice into the buffer 10,000 times, and the consumer will read from the buffer and
// then make sure we read the correct byte slice.

func Test1ProducerAnd1ConsumerAgain(t *testing.T) {
	// Creates a new ring buffer that's 256 slots and each slot 128 bytes long.
	r, err := New(128, 256)
	if err != nil {
		t.Fatal(err)
	}

	// Gets a single producer from the the ring buffer. If NewProducer() is called
	// the second time, an error will be returned.
	p, err := r.NewProducer()
	if err != nil {
		t.Fatal(err)
	}

	// Gets a singel consumer from the ring buffer. You can call NewConsumer() multiple
	// times and get back a new consumer each time. The consumers are independent and will
	// go through the ring buffers separately. In other words, each consumer will have
	// their own independent sequence tracker.
	c, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}

	// We are going to write 10,000 items into the buffer.
	var count int64 = 10000

	// Let's prepare the data to write. It's just a basic byte slice that's 256 bytes long.
	dataSize := 256
	data := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = byte(i % 256)
	}

	// Producer goroutine
	go func() {
		// Producer will put the same data slice into the buffer _count_ times
		for i := int64(0); i < count; i++ {
			if _, err := p.Put(data); err != nil {
				// Unfortuantely we have an issue here. If the producer gets an error
				// and exits, the consumer will continue to wait and not exit. In the
				// real-world, we need to notify all the consumers that there's been
				// an error and ensure they exit as well.
				t.Fatal(err)
			}
		}
	}()

	var total int64

	// Consumer goroutine

	// The consumer will also read from the buffer _count_times
	for i := int64(0); i < count; i++ {
		if out, err := c.Get(); err != nil {
			t.Fatal(err)
		} else {
			// Check to see if the byte slice we got is the same as the original data
			if !bytes.Equal(out.([]byte), data) {
				t.Fatalf("bytes not the same")
			}

			total++
		}
	}

	// Check to make sure the count matches
	if total != count {
		t.Fatalf("Expected to have read %d items, got %d\n", count, total)
	}
}

func Test1ProducerAnd2Consumer(t *testing.T) {
	// Creates a new ring buffer that's 256 slots and each slot 128 bytes long.
	r, err := New(128, 256)
	if err != nil {
		t.Fatal(err)
	}

	// Gets a single producer from the the ring buffer. If NewProducer() is called
	// the second time, an error will be returned.
	p, err := r.NewProducer()
	if err != nil {
		t.Fatal(err)
	}

	// Gets a singel consumer from the ring buffer. You can call NewConsumer() multiple
	// times and get back a new consumer each time. The consumers are independent and will
	// go through the ring buffers separately. In other words, each consumer will have
	// their own independent sequence tracker.
	c, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}

	c2, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}

	// We are going to write 10,000 items into the buffer.
	var count int64 = 10000

	// Let's prepare the data to write. It's just a basic byte slice that's 256 bytes long.
	dataSize := 256
	data := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = byte(i % 256)
	}

	// Producer goroutine
	go func() {
		// Producer will put the same data slice into the buffer _count_ times
		for i := int64(0); i < count; i++ {
			if _, err := p.Put(data); err != nil {
				// Unfortuantely we have an issue here. If the producer gets an error
				// and exits, the consumer will continue to wait and not exit. In the
				// real-world, we need to notify all the consumers that there's been
				// an error and ensure they exit as well.
				t.Fatal(err)
			}
		}
	}()

	// Consumer goroutine #1
	go func() {
		var total int64


		// The consumer will also read from the buffer _count_times
		for i := int64(0); i < count; i++ {
			if out, err := c.Get(); err != nil {
				t.Fatal(err)
			} else {
				// Check to see if the byte slice we got is the same as the original data
				if !bytes.Equal(out.([]byte), data) {
					t.Fatalf("bytes not the same")
				}

				total++
			}
		}

		// Check to make sure the count matches
		if total != count {
			t.Fatalf("Expected to have read %d items, got %d\n", count, total)
		}
	}()
	
	var total int64

	// Consumer goroutine #2

	// The consumer will also read from the buffer _count_times
	for i := int64(0); i < count; i++ {
		if out, err := c2.Get(); err != nil {
			t.Fatal(err)
		} else {
			// Check to see if the byte slice we got is the same as the original data
			if !bytes.Equal(out.([]byte), data) {
				t.Fatalf("bytes not the same")
			}

			total++
		}
	}

	// Check to make sure the count matches
	if total != count {
		t.Fatalf("Expected to have read %d items, got %d\n", count, total)
	}
}

func Benchmark1ProducerAnd1Consumer(b *testing.B) {
	r, err := New(128, 128)
	if err != nil {
		b.Fatal(err)
	}

	p, err := r.NewProducer()
	if err != nil {
		b.Fatal(err)
	}

	c, err := r.NewConsumer()
	if err != nil {
		b.Fatal(err)
	}

	var count int64 = int64(b.N)

	dataSize := 256
	data := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()

	// Producer goroutine
	go func() {
		for i := int64(0); i < count; i++ {
			if _, err := p.Put(data); err != nil {
				b.Fatal(err)
			}
		}
	}()

	var total int64

	for i := int64(0); i < count; i++ {
		if out, err := c.Get(); err != nil {
			b.Fatal(err)
		} else {
			if !bytes.Equal(out.([]byte), data) {
				b.Fatalf("bytes not the same")
			}

			total++
		}
	}

	if total != count {
		b.Fatalf("Expected to have read %d items, got %d\n", count, total)
	}
}

func Benchmark1ProducerAnd2Consumers(b *testing.B) {
	r, err := New(128, 128)
	if err != nil {
		b.Fatal(err)
	}

	p, err := r.NewProducer()
	if err != nil {
		b.Fatal(err)
	}

	c, err := r.NewConsumer()
	if err != nil {
		b.Fatal(err)
	}

	c2, err := r.NewConsumer()
	if err != nil {
		b.Fatal(err)
	}

	var count int64 = int64(b.N)

	dataSize := 256
	data := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()

	// Producer goroutine
	go func() {
		for i := int64(0); i < count; i++ {
			if _, err := p.Put(data); err != nil {
				b.Fatal(err)
			}
		}
	}()

	go func() {
		var total int64

		for i := int64(0); i < count; i++ {
			if out, err := c2.Get(); err != nil {
				b.Fatal(err)
			} else {
				if !bytes.Equal(out.([]byte), data) {
					b.Fatalf("bytes not the same")
				}

				total++
			}
		}

		if total != count {
			b.Fatalf("Expected to have read %d items, got %d\n", count, total)
		}
	}()
	
	var total int64

	for i := int64(0); i < count; i++ {
		if out, err := c.Get(); err != nil {
			b.Fatal(err)
		} else {
			if !bytes.Equal(out.([]byte), data) {
				b.Fatalf("bytes not the same")
			}

			total++
		}
	}

	if total != count {
		b.Fatalf("Expected to have read %d items, got %d\n", count, total)
	}
}

func BenchmarkChannels(b *testing.B) {
	dataSize := 256
	data := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = byte(i % 256)
	}

	ch := make(chan []byte, 128)
	go func() {
		for i := 0; i < b.N; i++ {
			tmp := make([]byte, dataSize)
			copy(tmp, data)
			ch <- tmp
		}
	}()

	for i := 0; i < b.N; i++ {
		out := <-ch
		if !bytes.Equal(out, data) {
			b.Fatalf("bytes not the same")
		}
	}
}
