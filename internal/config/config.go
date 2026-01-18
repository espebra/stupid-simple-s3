package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Privilege string

const (
	PrivilegeRead      Privilege = "read"
	PrivilegeReadWrite Privilege = "read-write"
)

type Credential struct {
	AccessKeyID     string    `yaml:"access_key_id"`
	SecretAccessKey string    `yaml:"secret_access_key"`
	Privileges      Privilege `yaml:"privileges"`
}

type Bucket struct {
	Name string `yaml:"name"`
}

type Storage struct {
	Path          string `yaml:"path"`
	MultipartPath string `yaml:"multipart_path"`
}

type Server struct {
	Address string `yaml:"address"`
}

type Config struct {
	Bucket      Bucket       `yaml:"bucket"`
	Storage     Storage      `yaml:"storage"`
	Server      Server       `yaml:"server"`
	Credentials []Credential `yaml:"credentials"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
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
