package api

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/espen/stupid-simple-s3/internal/config"
)

func TestMetricsListener(t *testing.T) {
	newConfig := func(metricsAddress string, pprofEnabled bool) *config.Config {
		return &config.Config{
			Server: config.Server{Address: ":5553", MetricsAddress: metricsAddress},
			Pprof:  config.Pprof{Enabled: pprofEnabled},
			Credentials: []config.Credential{
				{AccessKeyID: "AKIA", SecretAccessKey: "secret", Privileges: config.PrivilegeReadWrite},
			},
		}
	}

	get := func(h http.Handler, path string, user, pass string) int {
		req := httptest.NewRequest("GET", path, nil)
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w.Code
	}

	for _, metricsAddress := range []string{"", ":5554"} {
		t.Run("S3 listener does not serve /metrics with metrics address "+strconv.Quote(metricsAddress), func(t *testing.T) {
			_, store, cleanup := setupTestHandlers(t)
			defer cleanup()
			srv := NewServer(newConfig(metricsAddress, false), store)

			// /metrics is routed to S3 as bucket "metrics", so an
			// unauthenticated request is rejected.
			if code := get(srv.Handler(), "/metrics", "", ""); code != http.StatusForbidden {
				t.Errorf("status = %d, want %d", code, http.StatusForbidden)
			}
			if code := get(srv.Handler(), "/healthz", "", ""); code != http.StatusOK {
				t.Errorf("/healthz status = %d, want %d", code, http.StatusOK)
			}
		})
	}

	t.Run("metrics listener without pprof", func(t *testing.T) {
		h := NewMetricsServer(newConfig(":5554", false)).Handler

		if code := get(h, "/metrics", "", ""); code != http.StatusOK {
			t.Errorf("/metrics status = %d, want %d", code, http.StatusOK)
		}
		if code := get(h, "/debug/pprof/", "", ""); code != http.StatusNotFound {
			t.Errorf("/debug/pprof/ status = %d, want %d", code, http.StatusNotFound)
		}
	})

	t.Run("metrics listener with pprof", func(t *testing.T) {
		h := NewMetricsServer(newConfig(":5554", true)).Handler

		if code := get(h, "/debug/pprof/", "", ""); code != http.StatusOK {
			t.Errorf("/debug/pprof/ status = %d, want %d", code, http.StatusOK)
		}
		if code := get(h, "/debug/pprof/heap", "", ""); code != http.StatusOK {
			t.Errorf("/debug/pprof/heap status = %d, want %d", code, http.StatusOK)
		}
	})

	t.Run("metrics listener basic auth covers metrics and pprof", func(t *testing.T) {
		cfg := newConfig(":5554", true)
		cfg.MetricsAuth = config.MetricsAuth{Username: "admin", Password: "secret"}
		h := NewMetricsServer(cfg).Handler

		for _, path := range []string{"/metrics", "/debug/pprof/"} {
			if code := get(h, path, "", ""); code != http.StatusUnauthorized {
				t.Errorf("%s without auth status = %d, want %d", path, code, http.StatusUnauthorized)
			}
			if code := get(h, path, "admin", "secret"); code != http.StatusOK {
				t.Errorf("%s with auth status = %d, want %d", path, code, http.StatusOK)
			}
		}
	})
}
