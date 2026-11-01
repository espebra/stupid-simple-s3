package storage

import (
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// bucketNameRegex matches the bucket names ValidateBucketName must accept:
// 3 to 63 lowercase letters, digits and hyphens, starting and ending with a
// letter or digit.
var bucketNameRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

// FuzzValidateBucketName tests that ValidateBucketName accepts exactly the
// names bucketNameRegex matches.
//
// Run with: go test -fuzz=FuzzValidateBucketName -fuzztime=30s ./internal/storage/
func FuzzValidateBucketName(f *testing.F) {
	f.Add("test-bucket")
	f.Add("abc")
	f.Add("ab")
	f.Add(strings.Repeat("a", 63))
	f.Add(strings.Repeat("a", 64))
	f.Add("-abc")
	f.Add("abc-")
	f.Add("a-c")
	f.Add("Abc")
	f.Add("a.c")
	f.Add("a/c")
	f.Add("..")
	f.Add("abé")
	f.Add("abc\x00")

	f.Fuzz(func(t *testing.T, name string) {
		got := ValidateBucketName(name) == nil
		want := bucketNameRegex.MatchString(name)
		if got != want {
			t.Fatalf("ValidateBucketName(%q) accepted = %v, want %v", name, got, want)
		}
	})
}

// FuzzKeyToPath tests that ValidateKey only accepts keys that are not empty,
// at most 1024 bytes, without NUL bytes, not absolute and without ".."
// path components, and that keyToPath maps every accepted bucket and key to
// a directory directly below the bucket's objects shard directory.
//
// Run with: go test -fuzz=FuzzKeyToPath -fuzztime=30s ./internal/storage/
func FuzzKeyToPath(f *testing.F) {
	fs, cleanup := setupTestStorage(f)
	f.Cleanup(cleanup)

	f.Add("test-bucket", "file.txt")
	f.Add("test-bucket", "a/b/c")
	f.Add("test-bucket", "a//b/./c/")
	f.Add("test-bucket", "../x")
	f.Add("test-bucket", "a/../../x")
	f.Add("test-bucket", `a\..\..\x`)
	f.Add("test-bucket", "..")
	f.Add("test-bucket", "...")
	f.Add("test-bucket", "/etc/passwd")
	f.Add("test-bucket", "a\x00b")
	f.Add("test-bucket", "")
	f.Add("test-bucket", strings.Repeat("k", 1024))
	f.Add("test-bucket", strings.Repeat("k", 1025))
	f.Add("../x", "key")
	f.Add("a/b", "key")
	f.Add("", "key")

	f.Fuzz(func(t *testing.T, bucket, key string) {
		keyOK := ValidateKey(key) == nil
		if keyOK {
			if key == "" || len(key) > 1024 || strings.ContainsRune(key, 0) || strings.HasPrefix(key, "/") {
				t.Fatalf("ValidateKey(%q) accepted an invalid key", key)
			}
			for _, part := range strings.Split(strings.ReplaceAll(key, `\`, "/"), "/") {
				if part == ".." {
					t.Fatalf("ValidateKey(%q) accepted a .. component", key)
				}
			}
		}

		path, err := fs.keyToPath(bucket, key)
		if err != nil {
			return
		}
		if !keyOK || ValidateBucketName(bucket) != nil {
			t.Fatalf("keyToPath(%q, %q) accepted an invalid bucket or key", bucket, key)
		}
		objects := filepath.Join(fs.basePath, "buckets", bucket, "objects")
		if shard := filepath.Dir(filepath.Dir(path)); shard != objects {
			t.Fatalf("keyToPath(%q, %q) = %q, not two levels below %q", bucket, key, path, objects)
		}
	})
}

// FuzzUploadDir tests that uploadDir maps every upload ID, valid or not, to a
// directory inside the multipart directory, and that validateUploadID only
// accepts IDs that uploadDir maps to a directory directly below it.
//
// Run with: go test -fuzz=FuzzUploadDir -fuzztime=30s ./internal/storage/
func FuzzUploadDir(f *testing.F) {
	fs, cleanup := setupTestStorage(f)
	f.Cleanup(cleanup)

	f.Add("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	f.Add("6BA7B810-9DAD-11D1-80B4-00C04FD430C8")
	f.Add("{6ba7b810-9dad-11d1-80b4-00c04fd430c8}")
	f.Add("urn:uuid:6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	f.Add("6ba7b8109dad11d180b400c04fd430c8")
	f.Add("../6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	f.Add("6ba7b810-9dad-11d1-80b4-00c04fd430c8/..")
	f.Add("..")
	f.Add(".")
	f.Add("")
	f.Add("a/b")
	f.Add("/abs")

	f.Fuzz(func(t *testing.T, uploadID string) {
		dir := fs.uploadDir(uploadID)
		if dir != fs.multipartPath && !strings.HasPrefix(dir, fs.multipartPath+string(filepath.Separator)) {
			t.Fatalf("uploadDir(%q) = %q, outside %q", uploadID, dir, fs.multipartPath)
		}
		if validateUploadID(uploadID) != nil {
			return
		}
		if filepath.Dir(dir) != fs.multipartPath || filepath.Base(dir) != uploadID {
			t.Fatalf("validateUploadID accepted %q, which uploadDir maps to %q", uploadID, dir)
		}
	})
}
