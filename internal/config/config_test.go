package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sss-config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("valid config", func(t *testing.T) {
		configContent := `
bucket:
  name: "test-bucket"

storage:
  path: "/var/lib/sss/data"
  multipart_path: "/var/lib/sss/tmp"

server:
  address: ":8080"

credentials:
  - access_key_id: "AKIAIOSFODNN7EXAMPLE"
    secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    privileges: "read-write"
  - access_key_id: "AKIAREADONLY"
    secret_access_key: "readonlysecret1234567890123456789012"
    privileges: "read"
`
		configPath := filepath.Join(tmpDir, "valid.yaml")
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}

		cfg, err := Load(configPath)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if cfg.Bucket.Name != "test-bucket" {
			t.Errorf("Bucket.Name = %q, want %q", cfg.Bucket.Name, "test-bucket")
		}
		if cfg.Storage.Path != "/var/lib/sss/data" {
			t.Errorf("Storage.Path = %q, want %q", cfg.Storage.Path, "/var/lib/sss/data")
		}
		if cfg.Server.Address != ":8080" {
			t.Errorf("Server.Address = %q, want %q", cfg.Server.Address, ":8080")
		}
		if len(cfg.Credentials) != 2 {
			t.Errorf("len(Credentials) = %d, want 2", len(cfg.Credentials))
		}
		if cfg.Credentials[0].Privileges != PrivilegeReadWrite {
			t.Errorf("Credentials[0].Privileges = %q, want %q", cfg.Credentials[0].Privileges, PrivilegeReadWrite)
		}
		if cfg.Credentials[1].Privileges != PrivilegeRead {
			t.Errorf("Credentials[1].Privileges = %q, want %q", cfg.Credentials[1].Privileges, PrivilegeRead)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := Load("/nonexistent/config.yaml")
		if err == nil {
			t.Error("expected error for missing file")
		}
	})

	t.Run("invalid yaml", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "invalid.yaml")
		if err := os.WriteFile(configPath, []byte("not: valid: yaml: ["), 0644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}

		_, err := Load(configPath)
		if err == nil {
			t.Error("expected error for invalid YAML")
		}
	})

	t.Run("missing bucket name", func(t *testing.T) {
		configContent := `
bucket:
  name: ""
storage:
  path: "/data"
  multipart_path: "/tmp"
server:
  address: ":8080"
credentials:
  - access_key_id: "AKIA"
    secret_access_key: "secret"
    privileges: "read"
`
		configPath := filepath.Join(tmpDir, "no-bucket.yaml")
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}

		_, err := Load(configPath)
		if err == nil {
			t.Error("expected error for missing bucket name")
		}
	})

	t.Run("missing storage path", func(t *testing.T) {
		configContent := `
bucket:
  name: "bucket"
storage:
  path: ""
  multipart_path: "/tmp"
server:
  address: ":8080"
credentials:
  - access_key_id: "AKIA"
    secret_access_key: "secret"
    privileges: "read"
`
		configPath := filepath.Join(tmpDir, "no-storage.yaml")
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}

		_, err := Load(configPath)
		if err == nil {
			t.Error("expected error for missing storage path")
		}
	})

	t.Run("no credentials", func(t *testing.T) {
		configContent := `
bucket:
  name: "bucket"
storage:
  path: "/data"
  multipart_path: "/tmp"
server:
  address: ":8080"
credentials: []
`
		configPath := filepath.Join(tmpDir, "no-creds.yaml")
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}

		_, err := Load(configPath)
		if err == nil {
			t.Error("expected error for no credentials")
		}
	})

	t.Run("invalid privilege", func(t *testing.T) {
		configContent := `
bucket:
  name: "bucket"
storage:
  path: "/data"
  multipart_path: "/tmp"
server:
  address: ":8080"
credentials:
  - access_key_id: "AKIA"
    secret_access_key: "secret"
    privileges: "invalid"
`
		configPath := filepath.Join(tmpDir, "bad-priv.yaml")
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}

		_, err := Load(configPath)
		if err == nil {
			t.Error("expected error for invalid privilege")
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
