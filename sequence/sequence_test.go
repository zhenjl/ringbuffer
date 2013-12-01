// Copyright (c) 2013 Zhen, LLC. http://zhen.io. All rights reserved.
// Use of this source code is governed by the Apache 2.0 license.

package sequence

import (
	"log"
	"runtime"
	"testing"
)

var _ = log.Ldate

func TestSequenceGet(t *testing.T) {
	seq := NewSequence()
	if v, err := seq.Get(); err != nil {
		t.Fatal(err)
	} else if v != -1 {
		t.Fatalf("Expect v == -1, got %d", v)
	}
}

func TestSequenceSetAndGet(t *testing.T) {
	seq := NewSequence()
	seq.Set(1000)
	if v, err := seq.Get(); err != nil {
		t.Fatal(err)
	} else if v != 1000 {
		t.Fatalf("Expect v == 1000, got ", v)
	}
}

func TestSequenceDisruptorStyle(t *testing.T) {
	var ringSize = int64(16)
	var ring [16]int64
	var ringMask = ringSize - 1

	pseq := NewSequence()
	cseq := NewSequence()

	go func() {
		max := int64(0)

		for i := int64(0); i < ringSize*2; i++ {
			for i >= max {
				if tmp, err := cseq.Get(); err != nil {
					t.Fatal(err)
				} else {
					max = tmp + ringSize - 2
				}
			}

			ring[i&ringMask] = i
			pseq.Set(i + 1)
		}
	}()

	max := int64(0)

	for i := int64(0); i < ringSize*2; i++ {
		for i >= max {
			if tmp, err := pseq.Get(); err != nil {
				t.Fatal(err)
			} else {
				max = tmp
			}
		}

		val := ring[i&ringMask]
		cseq.Set(i)

		if val != i {
			t.Fatalf("Exepect val == %d, got %d", i, val)
		}
	}
}

func BenchmarkSequenceGet(b *testing.B) {
	seq := NewSequence()

	for i := 0; i < b.N; i++ {
		if _, err := seq.Get(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSequenceSet(b *testing.B) {
	seq := NewSequence()

	for i := 0; i < b.N; i++ {
		if err := seq.Set(int64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// Modeled after https://gist.github.com/tux21b/1218360 for testing.
func BenchmarkSequenceDisruptorStyle(b *testing.B) {
	var ringSize = int64(2048)
	var ring [2048]int64
	var ringMask = ringSize - 1

	pseq := NewSequence()
	cseq := NewSequence()

	go func() {
		var max = int64(0)
		for i := int64(0); i < int64(b.N); i++ {
			for i >= max {
				if tmp, err := cseq.Get(); err != nil {
					b.Fatal(err)
				} else {
					max = tmp + ringSize - 2
				}
				runtime.Gosched()
			}
			ring[i&ringMask] = i
			pseq.Set(i + 1)
		}
	}()

	var max = int64(0)

	for i := int64(0); i < int64(b.N); i++ {
		for i >= max {
			if tmp, err := pseq.Get(); err != nil {
				b.Fatal(err)
			} else {
				max = tmp
			}
			runtime.Gosched()
		}

		val := ring[i&ringMask]
		cseq.Set(i)

		if val != i {
			b.Fatal("val not corret")
		}
	}
}

// Copied from https://gist.github.com/tux21b/1218360 for testing.
func BenchmarkChannels(b *testing.B) {
	ch := make(chan int, 2048)
	go func() {
		for i := 0; i < b.N; i++ {
			ch <- int(i)
		}
	}()

	for i := 0; i < b.N; i++ {
		val := <-ch
		if val != int(i) {
			b.Fatal("invalid result")
		}
	}
}
