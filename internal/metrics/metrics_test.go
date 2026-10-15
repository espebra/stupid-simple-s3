package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestSetBuildInfo(t *testing.T) {
	SetBuildInfo("v1.2.3", "abc1234")

	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)

	want := `stupid_simple_s3_build_info{commit="abc1234",version="v1.2.3"} 1`
	if !strings.Contains(string(body), want) {
		t.Errorf("expected metrics output to contain %q", want)
	}
}
