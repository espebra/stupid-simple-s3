package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Privilege string

const (
	PrivilegeRead      Privilege = "read"
	PrivilegeReadWrite Privilege = "read-write"
)

type Credential struct {
	AccessKeyID     string
	SecretAccessKey string
	Privileges      Privilege
}

type Bucket struct {
	Name string
}

type Storage struct {
	Path          string
	MultipartPath string
}

// Limits contains resource limits for the service
type Limits struct {
	MaxObjectSize int64 // Maximum size of a single object in bytes (0 = unlimited)
	MaxPartSize   int64 // Maximum size of a single multipart part in bytes (0 = unlimited)
}


// DefaultMaxObjectSize is 5GB (S3's maximum for single PUT)
const DefaultMaxObjectSize = 5 * 1024 * 1024 * 1024

// DefaultMaxPartSize is 5GB (S3's maximum part size)
const DefaultMaxPartSize = 5 * 1024 * 1024 * 1024

type Cleanup struct {
	Enabled  bool
	Interval string
	MaxAge   string
}

// GetInterval returns the cleanup interval as a duration, defaulting to 1 hour
func (c *Cleanup) GetInterval() time.Duration {
	if c.Interval == "" {
		return time.Hour
	}
	d, err := time.ParseDuration(c.Interval)
	if err != nil {
		return time.Hour
	}
	return d
}

// GetMaxAge returns the max age for stale uploads, defaulting to 24 hours
func (c *Cleanup) GetMaxAge() time.Duration {
	if c.MaxAge == "" {
		return 24 * time.Hour
	}
	d, err := time.ParseDuration(c.MaxAge)
	if err != nil {
		return 24 * time.Hour
	}
	return d
}

type Server struct {
	Address        string
	TrustedProxies []string // List of trusted proxy IPs/CIDRs that can set X-Forwarded-For
}

type MetricsAuth struct {
	Username string
	Password string
}

// Enabled returns true if metrics authentication is configured
func (m *MetricsAuth) Enabled() bool {
	return m.Username != "" && m.Password != ""
}

type Config struct {
	Bucket      Bucket
	Storage     Storage
	Server      Server
	Credentials []Credential
	Cleanup     Cleanup
	MetricsAuth MetricsAuth
	Limits      Limits
}

// Load creates a configuration from environment variables.
// Environment variables:
//   - STUPID_HOST: Listen host (default: all interfaces)
//   - STUPID_PORT: Listen port (default: "5553")
//   - STUPID_BUCKET_NAME: Bucket name (required)
//   - STUPID_STORAGE_PATH: Storage path (default: "/var/lib/stupid-simple-s3/data")
//   - STUPID_MULTIPART_PATH: Multipart storage path (default: "/var/lib/stupid-simple-s3/tmp")
//   - STUPID_CLEANUP_ENABLED: Enable cleanup job (default: "true")
//   - STUPID_CLEANUP_INTERVAL: Cleanup interval (default: "1h")
//   - STUPID_CLEANUP_MAX_AGE: Max age for stale uploads (default: "24h")
//   - STUPID_RO_ACCESS_KEY: Read-only user access key
//   - STUPID_RO_SECRET_KEY: Read-only user secret key
//   - STUPID_RW_ACCESS_KEY: Read-write user access key
//   - STUPID_RW_SECRET_KEY: Read-write user secret key
//   - STUPID_METRICS_USERNAME: Username for /metrics basic auth (optional)
//   - STUPID_METRICS_PASSWORD: Password for /metrics basic auth (optional)
//   - STUPID_MAX_OBJECT_SIZE: Maximum object size in bytes (default: 5GB)
//   - STUPID_MAX_PART_SIZE: Maximum multipart part size in bytes (default: 5GB)
//   - STUPID_TRUSTED_PROXIES: Comma-separated list of trusted proxy IPs/CIDRs (optional)
func Load() (*Config, error) {
	host := os.Getenv("STUPID_HOST")
	port := os.Getenv("STUPID_PORT")
	if port == "" {
		port = "5553"
	}

	address := host + ":" + port

	storagePath := os.Getenv("STUPID_STORAGE_PATH")
	if storagePath == "" {
		storagePath = "/var/lib/stupid-simple-s3/data"
	}

	multipartPath := os.Getenv("STUPID_MULTIPART_PATH")
	if multipartPath == "" {
		multipartPath = "/var/lib/stupid-simple-s3/tmp"
	}

	// Parse trusted proxies
	var trustedProxies []string
	if proxyList := os.Getenv("STUPID_TRUSTED_PROXIES"); proxyList != "" {
		for _, proxy := range strings.Split(proxyList, ",") {
			proxy = strings.TrimSpace(proxy)
			if proxy != "" {
				trustedProxies = append(trustedProxies, proxy)
			}
		}
	}

	cfg := &Config{
		Bucket: Bucket{
			Name: os.Getenv("STUPID_BUCKET_NAME"),
		},
		Storage: Storage{
			Path:          storagePath,
			MultipartPath: multipartPath,
		},
		Server: Server{
			Address:        address,
			TrustedProxies: trustedProxies,
		},
		Cleanup: Cleanup{
			Enabled:  os.Getenv("STUPID_CLEANUP_ENABLED") != "false",
			Interval: getEnvOrDefault("STUPID_CLEANUP_INTERVAL", "1h"),
			MaxAge:   getEnvOrDefault("STUPID_CLEANUP_MAX_AGE", "24h"),
		},
		MetricsAuth: MetricsAuth{
			Username: os.Getenv("STUPID_METRICS_USERNAME"),
			Password: os.Getenv("STUPID_METRICS_PASSWORD"),
		},
		Limits: Limits{
			MaxObjectSize: parseEnvInt64("STUPID_MAX_OBJECT_SIZE", DefaultMaxObjectSize),
			MaxPartSize:   parseEnvInt64("STUPID_MAX_PART_SIZE", DefaultMaxPartSize),
		},
	}

	// Add read-only credential if both key and secret are provided
	roAccessKey := os.Getenv("STUPID_RO_ACCESS_KEY")
	roSecretKey := os.Getenv("STUPID_RO_SECRET_KEY")
	if roAccessKey != "" && roSecretKey != "" {
		cfg.Credentials = append(cfg.Credentials, Credential{
			AccessKeyID:     roAccessKey,
			SecretAccessKey: roSecretKey,
			Privileges:      PrivilegeRead,
		})
	}

	// Add read-write credential if both key and secret are provided
	rwAccessKey := os.Getenv("STUPID_RW_ACCESS_KEY")
	rwSecretKey := os.Getenv("STUPID_RW_SECRET_KEY")
	if rwAccessKey != "" && rwSecretKey != "" {
		cfg.Credentials = append(cfg.Credentials, Credential{
			AccessKeyID:     rwAccessKey,
			SecretAccessKey: rwSecretKey,
			Privileges:      PrivilegeReadWrite,
		})
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func (c *Config) validate() error {
	if c.Bucket.Name == "" {
		return fmt.Errorf("bucket.name is required")
	}
	if c.Storage.Path == "" {
		return fmt.Errorf("storage.path is required")
	}
	if c.Storage.MultipartPath == "" {
		return fmt.Errorf("storage.multipart_path is required")
	}
	if c.Server.Address == "" {
		return fmt.Errorf("server.address is required")
	}
	if len(c.Credentials) == 0 {
		return fmt.Errorf("at least one credential is required")
	}

	for i, cred := range c.Credentials {
		if cred.AccessKeyID == "" {
			return fmt.Errorf("credentials[%d].access_key_id is required", i)
		}
		if cred.SecretAccessKey == "" {
			return fmt.Errorf("credentials[%d].secret_access_key is required", i)
		}
		if cred.Privileges != PrivilegeRead && cred.Privileges != PrivilegeReadWrite {
			return fmt.Errorf("credentials[%d].privileges must be 'read' or 'read-write'", i)
		}
	}

	return nil
}

func (c *Config) GetCredential(accessKeyID string) *Credential {
	for _, cred := range c.Credentials {
		if cred.AccessKeyID == accessKeyID {
			return &cred
		}
	}
	return nil
}

func (c *Credential) CanWrite() bool {
	return c.Privileges == PrivilegeReadWrite
}

// Log prints the configuration to stdout, excluding secret values
func (c *Config) Log() {
	log.Println("Configuration:")
	log.Printf("  Server address: %s", c.Server.Address)
	log.Printf("  Bucket name: %s", c.Bucket.Name)
	log.Printf("  Storage path: %s", c.Storage.Path)
	log.Printf("  Multipart path: %s", c.Storage.MultipartPath)
	log.Printf("  Cleanup enabled: %t", c.Cleanup.Enabled)
	log.Printf("  Cleanup interval: %s", c.Cleanup.GetInterval())
	log.Printf("  Cleanup max age: %s", c.Cleanup.GetMaxAge())
	log.Printf("  Metrics auth enabled: %t", c.MetricsAuth.Enabled())
	log.Printf("  Max object size: %d bytes", c.Limits.MaxObjectSize)
	log.Printf("  Max part size: %d bytes", c.Limits.MaxPartSize)
	log.Printf("  Trusted proxies: %d configured", len(c.Server.TrustedProxies))
	log.Printf("  Credentials: %d configured", len(c.Credentials))
	for i, cred := range c.Credentials {
		log.Printf("    [%d] Access key: %s, Privileges: %s", i, cred.AccessKeyID, cred.Privileges)
	}
}
