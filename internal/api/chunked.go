package api

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// maxTrailerLines limits how many trailer lines are accepted after the final chunk
const maxTrailerLines = 16

// awsChunkedReader decodes AWS chunked transfer encoding
// Format: <hex-size>;chunk-signature=<sig>\r\n<data>\r\n...0;chunk-signature=<sig>\r\n[<trailers>]\r\n
// Trailers (e.g. x-amz-checksum-crc32:<value>\r\n) follow the final chunk when
// the client sends a trailing checksum.
type awsChunkedReader struct {
	reader       *bufio.Reader
	remaining    int64
	eof          bool
	totalRead    int64
	maxChunkSize int64
	trailers     map[string]string
}

// newAWSChunkedReader creates a new AWS chunked reader
func newAWSChunkedReader(r io.Reader, maxChunkSize int64) *awsChunkedReader {
	return &awsChunkedReader{
		reader:       bufio.NewReader(r),
		maxChunkSize: maxChunkSize,
	}
}

func (r *awsChunkedReader) Read(p []byte) (n int, err error) {
	if r.eof {
		return 0, io.EOF
	}

	for n < len(p) {
		if r.remaining == 0 {
			// Read next chunk header
			chunkSize, err := r.readChunkHeader()
			if err != nil {
				return n, err
			}

			if chunkSize == 0 {
				// Final chunk - read trailers up to the terminating empty line
				if err := r.readTrailers(); err != nil {
					return n, err
				}
				r.eof = true
				return n, io.EOF
			}

			r.remaining = chunkSize
		}

		// Read chunk data. toRead starts from the remaining space in p (an
		// int) and is only ever reduced, so it always fits back into an int.
		toRead := len(p) - n
		if int64(toRead) > r.remaining {
			if r.remaining > int64(math.MaxInt) {
				return n, fmt.Errorf("chunk size too large for platform int: %d", r.remaining)
			}
			toRead = int(r.remaining)
		}

		read, err := r.reader.Read(p[n : n+toRead])
		n += read
		r.remaining -= int64(read)
		r.totalRead += int64(read)

		if err != nil {
			return n, err
		}

		// If we've read the entire chunk, consume the trailing CRLF
		if r.remaining == 0 {
			if _, err := r.reader.ReadString('\n'); err != nil && err != io.EOF {
				return n, err
			}
		}
	}

	return n, nil
}

// readChunkHeader reads and parses the chunk header
// Format: <hex-size>;chunk-signature=<sig>\r\n
func (r *awsChunkedReader) readChunkHeader() (int64, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return 0, err
	}

	line = strings.TrimRight(line, "\r\n")

	// Split on semicolon to get size part
	parts := strings.SplitN(line, ";", 2)
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid chunk header")
	}

	size, err := strconv.ParseInt(parts[0], 16, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid chunk size: %w", err)
	}

	// Validate chunk size to prevent overflow and bypass of size limits
	if size < 0 {
		return 0, fmt.Errorf("invalid chunk size: negative value")
	}
	if r.maxChunkSize > 0 && size > r.maxChunkSize {
		return 0, fmt.Errorf("invalid chunk size: exceeds maximum allowed (%d > %d)", size, r.maxChunkSize)
	}

	return size, nil
}

// readTrailers reads the trailer lines after the final chunk until an empty
// line or end of input.
func (r *awsChunkedReader) readTrailers() error {
	for i := 0; i < maxTrailerLines; i++ {
		line, err := r.reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return nil
		}
		if name, value, ok := strings.Cut(line, ":"); ok {
			if r.trailers == nil {
				r.trailers = make(map[string]string)
			}
			r.trailers[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(value)
		}
		if err == io.EOF {
			return nil
		}
	}
	return fmt.Errorf("too many trailer lines")
}

// Trailer returns the value of the named trailer, or "" if it was not sent.
// It is only populated once the reader has reached the final chunk.
func (r *awsChunkedReader) Trailer(name string) string {
	return r.trailers[strings.ToLower(name)]
}

// TotalRead returns the total bytes of actual data read (excluding headers)
func (r *awsChunkedReader) TotalRead() int64 {
	return r.totalRead
}

// isAWSChunkedEncoding checks if the request uses AWS chunked encoding
func isAWSChunkedEncoding(contentEncoding, contentSha256 string) bool {
	if strings.Contains(contentEncoding, "aws-chunked") {
		return true
	}
	// Minio SDK uses STREAMING-AWS4-HMAC-SHA256-PAYLOAD in x-amz-content-sha256
	if strings.Contains(contentSha256, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD") {
		return true
	}
	return false
}
