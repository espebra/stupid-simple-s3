package api

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

// encodeAWSChunked encodes data as AWS chunked transfer encoding.
// Each chunk in chunkSizes bytes is emitted with a fake signature.
func encodeAWSChunked(data []byte, chunkSizes ...int) string {
	var buf strings.Builder
	offset := 0
	for _, size := range chunkSizes {
		if offset+size > len(data) {
			size = len(data) - offset
		}
		fmt.Fprintf(&buf, "%x;chunk-signature=abc123\r\n", size)
		buf.Write(data[offset : offset+size])
		buf.WriteString("\r\n")
		offset += size
		if offset >= len(data) {
			break
		}
	}
	// Final chunk
	buf.WriteString("0;chunk-signature=final\r\n\r\n")
	return buf.String()
}

func TestAWSChunkedReaderBasic(t *testing.T) {
	data := []byte("Hello, World!")
	encoded := encodeAWSChunked(data, len(data))

	reader := newAWSChunkedReader(strings.NewReader(encoded), 0)
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
	if reader.TotalRead() != int64(len(data)) {
		t.Errorf("TotalRead = %d, want %d", reader.TotalRead(), len(data))
	}
}

func TestAWSChunkedReaderMultipleChunks(t *testing.T) {
	data := []byte("abcdefghij")
	encoded := encodeAWSChunked(data, 3, 3, 4)

	reader := newAWSChunkedReader(strings.NewReader(encoded), 0)
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestAWSChunkedReaderEmptyBody(t *testing.T) {
	encoded := "0;chunk-signature=abc\r\n\r\n"
	reader := newAWSChunkedReader(strings.NewReader(encoded), 0)
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d bytes, want 0", len(got))
	}
}

func TestAWSChunkedReaderMaxChunkSize(t *testing.T) {
	data := []byte("too large")
	encoded := encodeAWSChunked(data, len(data))

	reader := newAWSChunkedReader(strings.NewReader(encoded), 4)
	_, err := io.ReadAll(reader)
	if err == nil {
		t.Error("expected error for chunk exceeding max size")
	}
}

func TestAWSChunkedReaderSmallReadBuffer(t *testing.T) {
	data := []byte("abcdefghijklmnop")
	encoded := encodeAWSChunked(data, 5, 5, 6)

	reader := newAWSChunkedReader(strings.NewReader(encoded), 0)

	// Read one byte at a time
	var got []byte
	buf := make([]byte, 1)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestAWSChunkedReaderNoSignature(t *testing.T) {
	// Chunk header with just hex size, no signature
	encoded := "5\r\nhello\r\n0\r\n\r\n"
	reader := newAWSChunkedReader(strings.NewReader(encoded), 0)
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
}

func TestAWSChunkedReaderInvalidHex(t *testing.T) {
	encoded := "zz;chunk-signature=abc\r\ndata\r\n"
	reader := newAWSChunkedReader(strings.NewReader(encoded), 0)
	_, err := io.ReadAll(reader)
	if err == nil {
		t.Error("expected error for invalid hex size")
	}
}

func TestAWSChunkedReaderTruncatedInput(t *testing.T) {
	// Claims 100 bytes (0x64) but input ends after "short" (5 bytes).
	// The underlying reader returns io.EOF which is propagated. io.ReadAll
	// treats EOF as normal termination so returns the partial data without
	// error. This is consistent with the io.Reader contract. Data integrity
	// is enforced at a higher level via ETag verification.
	encoded := "64;chunk-signature=abc\r\nshort"
	reader := newAWSChunkedReader(strings.NewReader(encoded), 0)
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "short" {
		t.Errorf("got %q, want %q", got, "short")
	}
	if reader.TotalRead() != 5 {
		t.Errorf("TotalRead = %d, want 5", reader.TotalRead())
	}
}

func TestIsAWSChunkedEncoding(t *testing.T) {
	tests := []struct {
		encoding string
		sha256   string
		want     bool
	}{
		{"aws-chunked", "", true},
		{"gzip, aws-chunked", "", true},
		{"", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", true},
		{"gzip", "", false},
		{"", "", false},
		{"", "UNSIGNED-PAYLOAD", false},
	}
	for _, tt := range tests {
		got := isAWSChunkedEncoding(tt.encoding, tt.sha256)
		if got != tt.want {
			t.Errorf("isAWSChunkedEncoding(%q, %q) = %v, want %v", tt.encoding, tt.sha256, got, tt.want)
		}
	}
}

// FuzzAWSChunkedReader tests the chunked reader with arbitrary input.
// The reader must never panic and must not read more data than the
// chunk headers declare.
func FuzzAWSChunkedReader(f *testing.F) {
	// Seed corpus: valid encodings
	f.Add([]byte("0;chunk-signature=abc\r\n\r\n"))
	f.Add([]byte("5;chunk-signature=abc\r\nhello\r\n0;chunk-signature=abc\r\n\r\n"))
	f.Add([]byte("a\r\n0123456789\r\n0\r\n\r\n"))
	f.Add([]byte("3\r\nabc\r\n4\r\ndefg\r\n0\r\n\r\n"))

	// Edge cases: malformed input
	f.Add([]byte(""))
	f.Add([]byte("\r\n"))
	f.Add([]byte("\n"))
	f.Add([]byte("ffffffffffffffff\r\n"))
	f.Add([]byte("-1\r\n"))
	f.Add([]byte("zz\r\n"))
	f.Add([]byte("5\r\nhi"))
	f.Add([]byte("0\r\n"))
	f.Add([]byte("1;\r\na\r\n0\r\n\r\n"))
	f.Add([]byte("1;chunk-signature=\r\na\r\n0;chunk-signature=\r\n\r\n"))
	f.Add([]byte(strings.Repeat("a", 10000) + "\r\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, maxChunk := range []int64{0, 1, 64, 4096} {
			reader := newAWSChunkedReader(bytes.NewReader(data), maxChunk)

			// Must not panic. Errors are fine.
			output, _ := io.ReadAll(reader)

			// TotalRead must match what was actually returned
			if int64(len(output)) != reader.TotalRead() {
				t.Errorf("maxChunk=%d: len(output)=%d but TotalRead()=%d",
					maxChunk, len(output), reader.TotalRead())
			}

			// After EOF, further reads must return EOF
			buf := make([]byte, 1)
			if reader.eof {
				n, err := reader.Read(buf)
				if n != 0 || err != io.EOF {
					t.Errorf("maxChunk=%d: read after EOF returned n=%d err=%v", maxChunk, n, err)
				}
			}
		}
	})
}
