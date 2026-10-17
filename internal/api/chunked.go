package api

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"

	"github.com/espen/stupid-simple-s3/internal/auth"
)

// maxTrailerLines limits how many trailer lines are accepted after the final chunk
const maxTrailerLines = 16

// maxChunkLineLength limits chunk header and trailer lines. Real lines, such
// as "<hex-size>;chunk-signature=<64 hex>" or a checksum trailer, are well
// under 200 bytes. It is also the size of the read buffer.
const maxChunkLineLength = 4096

// errChunkLineTooLong is returned when a chunk header or trailer line is
// longer than maxChunkLineLength
var errChunkLineTooLong = errors.New("aws-chunked line too long")

// awsChunkedReader decodes AWS chunked transfer encoding
// Format: <hex-size>;chunk-signature=<sig>\r\n<data>\r\n...0;chunk-signature=<sig>\r\n[<trailers>]\r\n
// Trailers (e.g. x-amz-checksum-sha256:<value>\r\n) follow the final chunk when
// the client sends a trailing checksum.
type awsChunkedReader struct {
	reader       *bufio.Reader
	remaining    int64
	eof          bool
	totalRead    int64
	maxChunkSize int64
	trailers     map[string]string

	// verifier checks chunk and trailer signatures when the upload is
	// signed (STREAMING-AWS4-HMAC-SHA256-PAYLOAD). nil skips the checks.
	verifier       *auth.ChunkVerifier
	chunkHash      hash.Hash
	chunkSignature string
}

// newAWSChunkedReader creates a new AWS chunked reader
func newAWSChunkedReader(r io.Reader, maxChunkSize int64) *awsChunkedReader {
	return &awsChunkedReader{
		reader:       bufio.NewReaderSize(r, maxChunkLineLength),
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
				// The final, empty chunk is signed like the others
				if r.verifier != nil {
					if err := r.verifier.VerifyChunk(r.chunkHash.Sum(nil), r.chunkSignature); err != nil {
						return n, err
					}
				}
				// Final chunk - read trailers up to the terminating empty line
				if err := r.readTrailers(); err != nil {
					return n, err
				}
				r.eof = true
				return n, io.EOF
			}

			r.remaining = chunkSize
		}

		// Read chunk data, at most the rest of the chunk. Slicing with the
		// int64 directly, only when it is smaller than the buffer, avoids
		// converting the client-supplied chunk size to an int.
		buf := p[n:]
		if int64(len(buf)) > r.remaining {
			buf = buf[:r.remaining]
		}

		read, err := r.reader.Read(buf)
		if r.verifier != nil {
			r.chunkHash.Write(buf[:read])
		}
		n += read
		r.remaining -= int64(read)
		r.totalRead += int64(read)

		if err != nil {
			return n, err
		}

		// If we've read the entire chunk, verify its signature and consume
		// the trailing CRLF
		if r.remaining == 0 {
			if r.verifier != nil {
				if err := r.verifier.VerifyChunk(r.chunkHash.Sum(nil), r.chunkSignature); err != nil {
					return n, err
				}
			}
			if _, err := r.readLine(); err != nil && err != io.EOF {
				return n, err
			}
		}
	}

	return n, nil
}

// readLine reads up to and including the next newline. Unlike
// bufio.Reader.ReadString, it fails with errChunkLineTooLong instead of
// buffering without limit when no newline arrives within maxChunkLineLength
// bytes.
func (r *awsChunkedReader) readLine() (string, error) {
	line, err := r.reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return "", errChunkLineTooLong
	}
	return string(line), err
}

// readChunkHeader reads and parses the chunk header
// Format: <hex-size>;chunk-signature=<sig>\r\n
func (r *awsChunkedReader) readChunkHeader() (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}

	line = strings.TrimRight(line, "\r\n")

	// Split on semicolon to get size part
	parts := strings.SplitN(line, ";", 2)
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid chunk header")
	}
	r.chunkSignature = ""
	if len(parts) == 2 {
		r.chunkSignature, _ = strings.CutPrefix(parts[1], "chunk-signature=")
	}
	if r.verifier != nil {
		if r.chunkHash == nil {
			r.chunkHash = sha256.New()
		}
		r.chunkHash.Reset()
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
// line or end of input, and verifies the trailer signature of a signed upload.
func (r *awsChunkedReader) readTrailers() error {
	// signed holds the trailer lines covered by x-amz-trailer-signature
	var signed strings.Builder
	for i := 0; i < maxTrailerLines; i++ {
		line, err := r.readLine()
		if err != nil && err != io.EOF {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return r.verifyTrailers(signed.String())
		}
		if name, value, ok := strings.Cut(line, ":"); ok {
			if r.trailers == nil {
				r.trailers = make(map[string]string)
			}
			name = strings.ToLower(strings.TrimSpace(name))
			r.trailers[name] = strings.TrimSpace(value)
			if name != trailerSignatureName {
				signed.WriteString(line + "\n")
			}
		}
		if err == io.EOF {
			return r.verifyTrailers(signed.String())
		}
	}
	return fmt.Errorf("too many trailer lines")
}

// trailerSignatureName is the trailer that signs the other trailers
const trailerSignatureName = "x-amz-trailer-signature"

// verifyTrailers checks the trailer signature of a signed upload that sent
// trailers. Without a verifier, or without trailers, there is nothing to check.
func (r *awsChunkedReader) verifyTrailers(signed string) error {
	if r.verifier == nil || signed == "" {
		return nil
	}
	return r.verifier.VerifyTrailer(signed, r.trailers[trailerSignatureName])
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
