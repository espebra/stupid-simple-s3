package api

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// awsChunkedReader decodes AWS chunked transfer encoding
// Format: <hex-size>;chunk-signature=<sig>\r\n<data>\r\n...0;chunk-signature=<sig>\r\n\r\n
type awsChunkedReader struct {
	reader    *bufio.Reader
	remaining int64
	eof       bool
	totalRead int64
}

// newAWSChunkedReader creates a new AWS chunked reader
func newAWSChunkedReader(r io.Reader) *awsChunkedReader {
	return &awsChunkedReader{
		reader: bufio.NewReader(r),
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
				// Final chunk - read trailing CRLF
				r.reader.ReadString('\n')
				r.eof = true
				return n, io.EOF
			}

			r.remaining = chunkSize
		}

		// Read chunk data
		toRead := int64(len(p) - n)
		if toRead > r.remaining {
			toRead = r.remaining
		}

		read, err := r.reader.Read(p[n : n+int(toRead)])
		n += read
		r.remaining -= int64(read)
		r.totalRead += int64(read)

		if err != nil {
			return n, err
		}

		// If we've read the entire chunk, consume the trailing CRLF
		if r.remaining == 0 {
			r.reader.ReadString('\n') // Read trailing \r\n
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

	return size, nil
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

// getRequestBody returns the appropriate reader for the request body
// If the request uses AWS chunked encoding, it wraps the body in a decoder
func getRequestBody(body io.ReadCloser, contentEncoding, contentSha256 string) io.Reader {
	if isAWSChunkedEncoding(contentEncoding, contentSha256) {
		return newAWSChunkedReader(body)
	}
	return body
}

// readAllWithChunkedSupport reads all data from a reader, handling AWS chunked encoding
func readAllWithChunkedSupport(r io.Reader, contentEncoding, contentSha256 string) ([]byte, error) {
	if isAWSChunkedEncoding(contentEncoding, contentSha256) {
		chunkedReader := newAWSChunkedReader(r)
		return io.ReadAll(chunkedReader)
	}
	return io.ReadAll(r)
}

// wrapBodyIfChunked wraps the request body if it uses AWS chunked encoding
// Returns the wrapped reader
func wrapBodyIfChunked(body io.ReadCloser, contentEncoding, contentSha256 string) io.Reader {
	if isAWSChunkedEncoding(contentEncoding, contentSha256) {
		return newAWSChunkedReader(body)
	}
	return body
}
