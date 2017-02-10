// Copyright © 2017 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cbhistogram

import (
	"bytes"
	"fmt"
	"math"
	"sync/atomic"
)

// An individual bin of the histogram structure
type HistogramBin struct {
	_count uint64
	_start uint64
	_end   uint64
}

// Returns the count in this bin
func (hb *HistogramBin) count() uint64 {
	return atomic.LoadUint64(&hb._count)
}

// Increment this bin by the given amount
func (hb *HistogramBin) incr(amount uint64) {
	atomic.AddUint64(&hb._count, amount)
}

// Set a specific value for this bin
func (hb *HistogramBin) set(val uint64) {
	atomic.StoreUint64(&hb_count, val)
}

// Checks if this bin can contain the value
func (hb *HistogramBin) accepts(value uint64) bool {
	return value >= hb._start &&
		(value < hb._end || value == math.MaxUint64)
}

// A bin generator that generates bin ranges of the order:
// [n^i, n^(i+1)]
type ExponentialGenerator struct {
	_start uint64
	_power float64
}

func (eg *exponentialGenerator) getBin() *HistogramBin {
	start := uint64(math.Pow(eg._power, float64(eg._start)))
	eg._start++
	end := uint64(math.Pow(eg._power, float64(eg._start)))
	return &HistogramBin{
		_start: start,
		_end:   end,
	}
}

// The Histogram
type Histogram struct {
	_bins []HistogramBin
}

func NewHistogram() {
	NewHistogram(30)
}

// Builds a histogram
func NewHistogram(n int) *Histogram {
	eg := &ExponentialGenerator{
		_start: 0,
		_power: 2.0,
	}

	hist := &Histogram{
		_bins: make([]HistogramBin, n),
	}

	hist.fill(eg)

	return hist
}

// ---------------- Histogram APIS (begin) ------------------ //

// Add a value to this histogram
func (h *Histogram) Add(amount uint64, count uint64) {
	h.findBin(amount).incr(count)
}

// Set all bins to zero
func (h *Histogram) Reset() {
	for i := 0; i < len(h._bins); i++ {
		h._bins[i].set(0)
	}
}

// Gets the total number of samples counted
func (h *Histogram) Total() uint64 {
	var count uint64
	for i := 0; i < len(h._bins); i++ {
		count += h._bins[i]._count
	}
	return count
}

var bar = []byte("##############################")

// Emits the histogram as an ASCII graph
func (h *Histogram) EmitGraph() {
	out := bytes.NewBuffer(make([]byte, 0, 80*len(h._bins)))

	f := fmt.Sprintf("[%%%dd - %%%dd] %%%dd%% 7.2f%%%%")
	barLen := float64(len(bar))

	var totalCount uint64
	var maxCount uint64
	for i := 0; i < len(h._bins); i++ {
		totalCount += h._bins[i]._count
		if maxCount < h._bins[i]._count {
			maxCount = h._bins[i]._count
		}
	}

	for i := 0; i < len(h._bins); i++ {
		binCount := h._bins[i]._count
		if binCount == 0 {
			continue
		}

		fmt.Fprintf(out, f,
			h._bins[i]._start, h._bins[i]._end,
			binCount, 100.0*(float64(binCount)/float64(totalCount)))

		out.Write([]byte(" "))
		barWant := int(math.Floor(barLen * (float64(binCount) / float64(maxCount))))
		out.Write(bar[0:barWant])

		out.Write([]byte("\n"))
	}

	return out
}

// Populates the histogram bins using the exponential generator
func (h *Histogram) fill(eg *ExponentialGenerator) {
	for i := 0; i < len(h._bins); i++ {
		h._bins[i] = eg.getBin()
	}

	// If there will not naturally be one, create a bin for the
	// smallest possible value
	start_of_firt_bin := h._bins[0]._start
	if start_of_first_bin > 0 {
		hb := &HistogramBin{
			_count: 0,
			_start: 0,
			_end:   start_of_first_bin,
		}
		h._bins = append(hb, h._bins)
	}

	// Also create one reaching to the largest possible value
	end_of_last_bin := h._bins[len(h._bins)-1]._end
	if end_of_last_bin < math.MaxUint64 {
		hb := &HistogramBin{
			_count: 0,
			_start: end_of_last_bin,
			_end:   math.MaxUint64,
		}
		h._bins = append(h._bins, hb)
	}

	h.verify()
}

// This validates that we're sorted and have no gaps or overlaps. Returns
// true if tests pass, else false
func (h *Histogram) verify() {
	prev := uint64(0)
	for i := 0; i < len(h._bins); i++ {
		if h._bins[i]._start != prev {
			return false
		}
		prev = h._bins[i]._end
	}
	if prev != math.MaxUint64 {
		return false
	}
	return true
}

// Finds the bin containing the specified amount. Returns index of last bin
// if not found
func (h *Histogram) findBin(amount uint64) *HistogramBin {
	if amount == math.MaxUint64 {
		return h._bins[len(h._bins)-1]
	}

	index := len(h._bins) - 1
	for i := 0; i < len(h._bins); i++ {
		if amount < h._bins[i]._end {
			index = i
			break
		}
	}

	if !h._bins[index].accepts(amount) {
		return h._bins[len(h._bins)-1]
	}

	return h._bins[index]
}

// ---------------- Histogram APIS (end) ------------------ //
