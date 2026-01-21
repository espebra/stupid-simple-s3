package config

import (
	"os"
	"testing"
)

func TestLoad(t *testing.T) {
	// Save original environment and restore after test
	origEnv := map[string]string{
		"STUPID_HOST":             os.Getenv("STUPID_HOST"),
		"STUPID_PORT":             os.Getenv("STUPID_PORT"),
		"STUPID_BUCKET_NAME":      os.Getenv("STUPID_BUCKET_NAME"),
		"STUPID_STORAGE_PATH":     os.Getenv("STUPID_STORAGE_PATH"),
		"STUPID_MULTIPART_PATH":   os.Getenv("STUPID_MULTIPART_PATH"),
		"STUPID_CLEANUP_ENABLED":  os.Getenv("STUPID_CLEANUP_ENABLED"),
		"STUPID_CLEANUP_INTERVAL": os.Getenv("STUPID_CLEANUP_INTERVAL"),
		"STUPID_CLEANUP_MAX_AGE":  os.Getenv("STUPID_CLEANUP_MAX_AGE"),
		"STUPID_RO_ACCESS_KEY":    os.Getenv("STUPID_RO_ACCESS_KEY"),
		"STUPID_RO_SECRET_KEY":    os.Getenv("STUPID_RO_SECRET_KEY"),
		"STUPID_RW_ACCESS_KEY":    os.Getenv("STUPID_RW_ACCESS_KEY"),
		"STUPID_RW_SECRET_KEY":    os.Getenv("STUPID_RW_SECRET_KEY"),
	}
	defer func() {
		for k, v := range origEnv {
			if v == "" {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, v)
			}
		}
	}()

	clearEnv := func() {
		for k := range origEnv {
			os.Unsetenv(k)
		}
	}

	t.Run("valid config with read-write credential", func(t *testing.T) {
		clearEnv()
		os.Setenv("STUPID_BUCKET_NAME", "test-bucket")
		os.Setenv("STUPID_STORAGE_PATH", "/var/lib/data")
		os.Setenv("STUPID_MULTIPART_PATH", "/var/lib/tmp")
		os.Setenv("STUPID_RW_ACCESS_KEY", "AKIAIOSFODNN7EXAMPLE")
		os.Setenv("STUPID_RW_SECRET_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if cfg.Bucket.Name != "test-bucket" {
			t.Errorf("Bucket.Name = %q, want %q", cfg.Bucket.Name, "test-bucket")
		}
		if cfg.Storage.Path != "/var/lib/data" {
			t.Errorf("Storage.Path = %q, want %q", cfg.Storage.Path, "/var/lib/data")
		}
		if cfg.Server.Address != ":5553" {
			t.Errorf("Server.Address = %q, want %q", cfg.Server.Address, ":5553")
		}
		if len(cfg.Credentials) != 1 {
			t.Errorf("len(Credentials) = %d, want 1", len(cfg.Credentials))
		}
		if cfg.Credentials[0].Privileges != PrivilegeReadWrite {
			t.Errorf("Credentials[0].Privileges = %q, want %q", cfg.Credentials[0].Privileges, PrivilegeReadWrite)
		}
	})

	t.Run("valid config with both credentials", func(t *testing.T) {
		clearEnv()
		os.Setenv("STUPID_BUCKET_NAME", "test-bucket")
		os.Setenv("STUPID_STORAGE_PATH", "/var/lib/data")
		os.Setenv("STUPID_MULTIPART_PATH", "/var/lib/tmp")
		os.Setenv("STUPID_RO_ACCESS_KEY", "AKIAREADONLY")
		os.Setenv("STUPID_RO_SECRET_KEY", "readonlysecret")
		os.Setenv("STUPID_RW_ACCESS_KEY", "AKIAREADWRITE")
		os.Setenv("STUPID_RW_SECRET_KEY", "readwritesecret")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(cfg.Credentials) != 2 {
			t.Errorf("len(Credentials) = %d, want 2", len(cfg.Credentials))
		}
		if cfg.Credentials[0].Privileges != PrivilegeRead {
			t.Errorf("Credentials[0].Privileges = %q, want %q", cfg.Credentials[0].Privileges, PrivilegeRead)
		}
		if cfg.Credentials[1].Privileges != PrivilegeReadWrite {
			t.Errorf("Credentials[1].Privileges = %q, want %q", cfg.Credentials[1].Privileges, PrivilegeReadWrite)
		}
	})

	t.Run("custom host and port", func(t *testing.T) {
		clearEnv()
		os.Setenv("STUPID_HOST", "127.0.0.1")
		os.Setenv("STUPID_PORT", "9000")
		os.Setenv("STUPID_BUCKET_NAME", "test-bucket")
		os.Setenv("STUPID_STORAGE_PATH", "/var/lib/data")
		os.Setenv("STUPID_MULTIPART_PATH", "/var/lib/tmp")
		os.Setenv("STUPID_RW_ACCESS_KEY", "AKIA")
		os.Setenv("STUPID_RW_SECRET_KEY", "secret")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if cfg.Server.Address != "127.0.0.1:9000" {
			t.Errorf("Server.Address = %q, want %q", cfg.Server.Address, "127.0.0.1:9000")
		}
	})

	t.Run("cleanup enabled by default", func(t *testing.T) {
		clearEnv()
		os.Setenv("STUPID_BUCKET_NAME", "test-bucket")
		os.Setenv("STUPID_RW_ACCESS_KEY", "AKIA")
		os.Setenv("STUPID_RW_SECRET_KEY", "secret")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if !cfg.Cleanup.Enabled {
			t.Error("Cleanup.Enabled = false, want true (default)")
		}
	})

	t.Run("cleanup disabled explicitly", func(t *testing.T) {
		clearEnv()
		os.Setenv("STUPID_BUCKET_NAME", "test-bucket")
		os.Setenv("STUPID_RW_ACCESS_KEY", "AKIA")
		os.Setenv("STUPID_RW_SECRET_KEY", "secret")
		os.Setenv("STUPID_CLEANUP_ENABLED", "false")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if cfg.Cleanup.Enabled {
			t.Error("Cleanup.Enabled = true, want false")
		}
	})

	t.Run("cleanup custom interval and max age", func(t *testing.T) {
		clearEnv()
		os.Setenv("STUPID_BUCKET_NAME", "test-bucket")
		os.Setenv("STUPID_RW_ACCESS_KEY", "AKIA")
		os.Setenv("STUPID_RW_SECRET_KEY", "secret")
		os.Setenv("STUPID_CLEANUP_INTERVAL", "30m")
		os.Setenv("STUPID_CLEANUP_MAX_AGE", "12h")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if cfg.Cleanup.Interval != "30m" {
			t.Errorf("Cleanup.Interval = %q, want %q", cfg.Cleanup.Interval, "30m")
		}
		if cfg.Cleanup.MaxAge != "12h" {
			t.Errorf("Cleanup.MaxAge = %q, want %q", cfg.Cleanup.MaxAge, "12h")
		}
	})

	t.Run("missing bucket name", func(t *testing.T) {
		clearEnv()
		os.Setenv("STUPID_RW_ACCESS_KEY", "AKIA")
		os.Setenv("STUPID_RW_SECRET_KEY", "secret")

		_, err := Load()
		if err == nil {
			t.Error("expected error for missing bucket name")
		}
	})

	t.Run("default storage paths", func(t *testing.T) {
		clearEnv()
		os.Setenv("STUPID_BUCKET_NAME", "test-bucket")
		os.Setenv("STUPID_RW_ACCESS_KEY", "AKIA")
		os.Setenv("STUPID_RW_SECRET_KEY", "secret")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if cfg.Storage.Path != "/var/lib/stupid-simple-s3/data" {
			t.Errorf("Storage.Path = %q, want %q", cfg.Storage.Path, "/var/lib/stupid-simple-s3/data")
		}
		if cfg.Storage.MultipartPath != "/var/lib/stupid-simple-s3/tmp" {
			t.Errorf("Storage.MultipartPath = %q, want %q", cfg.Storage.MultipartPath, "/var/lib/stupid-simple-s3/tmp")
		}
	})

	t.Run("no credentials", func(t *testing.T) {
		clearEnv()
		os.Setenv("STUPID_BUCKET_NAME", "test-bucket")

		_, err := Load()
		if err == nil {
			t.Error("expected error for no credentials")
		}
	})

	t.Run("partial read-only credential ignored", func(t *testing.T) {
		clearEnv()
		os.Setenv("STUPID_BUCKET_NAME", "test-bucket")
		os.Setenv("STUPID_RO_ACCESS_KEY", "AKIA") // missing secret
		os.Setenv("STUPID_RW_ACCESS_KEY", "AKIA2")
		os.Setenv("STUPID_RW_SECRET_KEY", "secret2")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Should only have the read-write credential
		if len(cfg.Credentials) != 1 {
			t.Errorf("len(Credentials) = %d, want 1", len(cfg.Credentials))
		}
	})
}

func TestGetCredential(t *testing.T) {
	cfg := &Config{
		Credentials: []Credential{
			{
				AccessKeyID:     "AKIA1",
				SecretAccessKey: "secret1",
				Privileges:      PrivilegeReadWrite,
			},
			{
				AccessKeyID:     "AKIA2",
				SecretAccessKey: "secret2",
				Privileges:      PrivilegeRead,
			},
		},
	}

	t.Run("existing credential", func(t *testing.T) {
		cred := cfg.GetCredential("AKIA1")
		if cred == nil {
			t.Fatal("expected to find credential")
		}
		if cred.SecretAccessKey != "secret1" {
			t.Errorf("SecretAccessKey = %q, want %q", cred.SecretAccessKey, "secret1")
		}
	})

	t.Run("second credential", func(t *testing.T) {
		cred := cfg.GetCredential("AKIA2")
		if cred == nil {
			t.Fatal("expected to find credential")
		}
		if cred.Privileges != PrivilegeRead {
			t.Errorf("Privileges = %q, want %q", cred.Privileges, PrivilegeRead)
		}
	})

	t.Run("nonexistent credential", func(t *testing.T) {
		cred := cfg.GetCredential("NONEXISTENT")
		if cred != nil {
			t.Error("expected nil for nonexistent credential")
		}
	})
}

func TestCanWrite(t *testing.T) {
	tests := []struct {
		privilege Privilege
		canWrite  bool
	}{
		{PrivilegeReadWrite, true},
		{PrivilegeRead, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.privilege), func(t *testing.T) {
			cred := &Credential{Privileges: tt.privilege}
			if cred.CanWrite() != tt.canWrite {
				t.Errorf("CanWrite() = %v, want %v", cred.CanWrite(), tt.canWrite)
			}
		})
	}
}

func TestCleanupGetInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval string
		want     string
	}{
		{"empty defaults to 1h", "", "1h0m0s"},
		{"valid duration", "30m", "30m0s"},
		{"valid duration hours", "2h", "2h0m0s"},
		{"invalid duration defaults to 1h", "invalid", "1h0m0s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cleanup{Interval: tt.interval}
			got := c.GetInterval().String()
			if got != tt.want {
				t.Errorf("GetInterval() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCleanupGetMaxAge(t *testing.T) {
	tests := []struct {
		name   string
		maxAge string
		want   string
	}{
		{"empty defaults to 24h", "", "24h0m0s"},
		{"valid duration", "12h", "12h0m0s"},
		{"valid duration minutes", "30m", "30m0s"},
		{"invalid duration defaults to 24h", "invalid", "24h0m0s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cleanup{MaxAge: tt.maxAge}
			got := c.GetMaxAge().String()
			if got != tt.want {
				t.Errorf("GetMaxAge() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMetricsAuthEnabled(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
		want     bool
	}{
		{"both set", "admin", "secret", true},
		{"only username", "admin", "", false},
		{"only password", "", "secret", false},
		{"neither set", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MetricsAuth{Username: tt.username, Password: tt.password}
			if got := m.Enabled(); got != tt.want {
				t.Errorf("Enabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigLog(t *testing.T) {
	cfg := &Config{
		Bucket: Bucket{Name: "test-bucket"},
		Storage: Storage{
			Path:          "/var/lib/data",
			MultipartPath: "/var/lib/tmp",
		},
		Server: Server{Address: ":5553"},
		Cleanup: Cleanup{
			Enabled:  true,
			Interval: "1h",
			MaxAge:   "24h",
		},
		MetricsAuth: MetricsAuth{
			Username: "admin",
			Password: "secret",
		},
		Credentials: []Credential{
			{AccessKeyID: "AKIA1", SecretAccessKey: "secret1", Privileges: PrivilegeReadWrite},
			{AccessKeyID: "AKIA2", SecretAccessKey: "secret2", Privileges: PrivilegeRead},
		},
	}

	// LogConfiguration() should not panic - just verify it runs
	cfg.LogConfiguration()
}
