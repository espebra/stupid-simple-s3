package main

import (
	"math"
	"testing"
)

func TestFitLine(t *testing.T) {
	// Checkpoints after the baseline, as operations and MiB of live heap,
	// from runs on fryer11
	tests := []struct {
		name         string
		pts          [][2]float64
		slope, error float64
		leak         bool
	}{
		{
			name:  "upload lock leak, 8h unlimited",
			pts:   [][2]float64{{3327723, 29.3}, {7124889, 54.4}, {11005765, 76.3}, {14871689, 99.2}, {18722108, 121.2}},
			slope: 6.2, error: 0.1, leak: true,
		},
		{
			name:  "upload lock leak, 30m limited",
			pts:   [][2]float64{{48390, 6.1}, {96536, 6.7}, {143046, 5.9}, {190507, 6.5}, {237642, 9.0}, {283452, 8.3}},
			slope: 11.8, error: 4.7, leak: true,
		},
		{
			name:  "fixed, 10m limited",
			pts:   [][2]float64{{19428, 4.5}, {38367, 4.8}, {57971, 7.1}, {75572, 5.0}, {95202, 6.2}},
			slope: 20.5, error: 18.8, leak: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var samples []sample
			for _, p := range tt.pts {
				samples = append(samples, sample{ops: int64(p[0]), heap: p[1] * (1 << 20)})
			}
			slope, stderr := fitLine(samples)
			if math.Abs(slope-tt.slope) > 0.1 || math.Abs(stderr-tt.error) > 0.1 {
				t.Errorf("slope %.2f ± %.2f, want %.1f ± %.1f", slope, stderr, tt.slope, tt.error)
			}
			if leak := slope-2*stderr > 1; leak != tt.leak {
				t.Errorf("judged a leak: %v, want %v", leak, tt.leak)
			}
		})
	}

	if slope, stderr := fitLine([]sample{{ops: 5, heap: 1}, {ops: 5, heap: 2}}); slope != 0 || stderr != 0 {
		t.Errorf("same operation count: slope %g ± %g, want 0", slope, stderr)
	}
}
