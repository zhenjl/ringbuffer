// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

package sequence

import (
	"log"
	"testing"
)

var _ = log.Ldate

func TestErrNotPowerOfTwo(t *testing.T) {
	_, err := NewProducer(12)
	if err != ErrNotPowerOfTwo {
		t.Fatal("Expect ErrNotPowerOfTwo, got " + err.Error())
	}
}

func TestErrNotPositiveInteger(t *testing.T) {
	p, err := NewProducer(16)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := p.Next(0); err != ErrNotPositiveInteger {
		t.Fatal("Expect ErrNotPositiveInteger, got " + err.Error())
	}
}

func TestGet(t *testing.T) {
	p, err := NewProducer(128)
	if err != nil {
		t.Fatal(err)
	}

	if v, err := p.Get(); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatalf("Expect v == -1, got %d", v)
	}
}

func TestSetAndGet(t *testing.T) {
	p, err := NewProducer(128)
	if err != nil {
		t.Fatal(err)
	}

	p.Set(1000)
	if v, err := p.Get(); err != nil {
		t.Fatal(err)
	} else if v != 1000 {
		t.Fatalf("Expect v == 1000, got ", v)
	}
}

func Test1ProducerAnd1Consumer(t *testing.T) {
	const ringSize = 128
	var ring [ringSize]int64
	var ringMask int64 = ringSize - 1

	pseq, err := NewProducer(ringSize)
	if err != nil {
		t.Fatal(err)
	}

	cseq, err := NewConsumer(ringSize)
	if err != nil {
		t.Fatal(err)
	}

	pseq.AddGatingSequence(cseq)
	cseq.AddGatingSequence(pseq)

	var count int64 = 100000

	// Producer goroutine
	go func() {
		for seq, err := pseq.Request(1); seq < count; seq, err = pseq.Request(1) {
			if err != nil {
				t.Fatal(err)
			} else {
				ring[seq&ringMask] = seq
				//log.Printf("producer: commit %d\n", seq)
				pseq.Commit(seq)
			}
		}
	}()

	var total int64

	// Consumer goroutine
	for seq, err := cseq.Request(1); seq < count; seq, err = cseq.Request(1) {
		if err != nil {
			t.Fatal(err)
		} else {
			val := ring[seq&ringMask]
			//log.Printf("consumer: commit %d\n", seq)
			cseq.Commit(seq)

			if val != seq {
				t.Fatalf("Expect val == %d, got %d", seq, val)
			}

			total++
		}

		if seq+1 >= count {
			break
		}
	}

	if total != count {
		t.Fatalf("Expected to have read %d items, got %d\n", count, total)
	}
}

func Test1ProducerAnd2Consumer(t *testing.T) {
	const ringSize = 128
	var ring [ringSize]int64
	var ringMask int64 = ringSize - 1

	pseq, err := NewProducer(ringSize)
	if err != nil {
		t.Fatal(err)
	}

	cseq, err := NewConsumer(ringSize)
	if err != nil {
		t.Fatal(err)
	}

	cseq2, err := NewConsumer(ringSize)
	if err != nil {
		t.Fatal(err)
	}

	pseq.AddGatingSequence(cseq)
	pseq.AddGatingSequence(cseq2)
	cseq.AddGatingSequence(pseq)
	cseq2.AddGatingSequence(pseq)

	var count int64 = 1000

	// Producer goroutine
	go func() {
		for seq, err := pseq.Request(1); seq < count; seq, err = pseq.Request(1) {
			if err != nil {
				t.Fatal(err)
			} else {
				ring[seq&ringMask] = seq
				//log.Printf("producer: commit %d\n", seq)
				pseq.Commit(seq)
			}
		}
	}()

	// Consumer goroutine 1
	go func() {
		var total int64

		for seq, err := cseq.Request(1); seq < count; seq, err = cseq.Request(1) {
			if err != nil {
				t.Fatal(err)
			} else {
				val := ring[seq&ringMask]
				//log.Printf("consumer1: commit %d\n", seq)
				cseq.Commit(seq)

				if val != seq {
					t.Fatalf("Expect val == %d, got %d", seq, val)
				}

				total++
			}

			if seq+1 >= count {
				break
			}
		}

		if total != count {
			t.Fatalf("Expected to have read %d items, got %d\n", count, total)
		}
	}()

	// Consumer goroutine 2
	var total int64

	for seq, err := cseq2.Request(1); seq < count; seq, err = cseq2.Request(1) {
		if err != nil {
			t.Fatal(err)
		} else {
			val := ring[seq&ringMask]
			//log.Printf("consumer2: commit %d\n", seq)
			cseq2.Commit(seq)

			if val != seq {
				t.Fatalf("Expect val == %d, got %d", seq, val)
			}

			total++
		}

		if seq+1 >= count {
			break
		}
	}

	if total != count {
		t.Fatalf("Expected to have read %d items, got %d\n", count, total)
	}
}

func Benchmark1ProducerAnd1Consumer(b *testing.B) {
	const ringSize = 128
	var ring [ringSize]int64
	var ringMask int64 = ringSize - 1

	pseq, err := NewProducer(ringSize)
	if err != nil {
		b.Fatal(err)
	}

	cseq, err := NewConsumer(ringSize)
	if err != nil {
		b.Fatal(err)
	}

	pseq.AddGatingSequence(cseq)
	cseq.AddGatingSequence(pseq)

	// Producer goroutine
	go func() {
		for seq, err := pseq.Request(1); seq < int64(b.N); seq, err = pseq.Request(1) {
			if err != nil {
				b.Fatal(err)
			} else {
				ring[seq&ringMask] = seq
				//log.Printf("producer: commit %d\n", seq)
				pseq.Commit(seq)
			}
		}
	}()

	var total int64

	// Consumer goroutine
	for seq, err := cseq.Request(1); seq < int64(b.N); seq, err = cseq.Request(1) {
		if err != nil {
			b.Fatal(err)
		} else {
			val := ring[seq&ringMask]
			//log.Printf("consumer: commit %d\n", seq)
			cseq.Commit(seq)

			if val != seq {
				b.Fatalf("Expect val == %d, got %d", seq, val)
			}

			total++
		}

		// If the next sequence number is bigger than the test count, then stop,
		// otherwise we will request the next sequence and we will end up in an
		// infinite loop
		if seq+1 >= int64(b.N) {
			break
		}
	}

	if total != int64(b.N) {
		b.Fatalf("Expected to have read %d items, got %d\n", int64(b.N), total)
	}
}
