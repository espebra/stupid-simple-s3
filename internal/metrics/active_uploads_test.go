package metrics

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// gatherActiveUploadsGauge returns the value of the named gauge in the default registry
func gatherActiveUploadsGauge(t *testing.T, name string) (float64, bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		if len(mf.GetMetric()) != 1 {
			t.Fatalf("%s has %d series, want 1", name, len(mf.GetMetric()))
		}
		return mf.GetMetric()[0].GetGauge().GetValue(), true
	}
	return 0, false
}

// The count reported by the registered gauge. RegisterMultipartUploadsActive
// registers with the default registry, so it can only be called once per
// process; registering once keeps the test working with -count=N.
var (
	activeUploadsCount    atomic.Int64
	activeUploadsCalls    atomic.Int64
	registerActiveUploads sync.Once
)

// TestRegisterMultipartUploadsActive tests the gauge that reports the number
// of active multipart uploads
func TestRegisterMultipartUploadsActive(t *testing.T) {
	const name = "stupid_simple_s3_multipart_uploads_active"
	registerActiveUploads.Do(func() {
		if _, ok := gatherActiveUploadsGauge(t, name); ok {
			t.Fatalf("%s is registered before RegisterMultipartUploadsActive", name)
		}
		RegisterMultipartUploadsActive(func() int {
			activeUploadsCalls.Add(1)
			return int(activeUploadsCount.Load())
		})
	})

	// The count is queried on every scrape
	callsBefore := activeUploadsCalls.Load()
	for _, want := range []int64{0, 3, 1, 0} {
		activeUploadsCount.Store(want)
		got, ok := gatherActiveUploadsGauge(t, name)
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		if got != float64(want) {
			t.Errorf("%s = %v, want %d", name, got, want)
		}
	}
	if got := activeUploadsCalls.Load() - callsBefore; got != 4 {
		t.Errorf("count function called %d times for 4 scrapes, want 4", got)
	}

	t.Run("second registration panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("registering the gauge twice did not panic")
			}
		}()
		RegisterMultipartUploadsActive(func() int { return 0 })
	})
}
