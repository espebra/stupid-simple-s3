package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand/v2"
)

// headerSize is the size of the header at the start of every object. The
// header names the key, seed and size the object was generated from, so
// that an object read back can be checked on its own, even where several
// writers race and the expected content is not known.
const headerSize = 32

var magic = [4]byte{'S', 'O', 'A', 'K'}

// content describes an object: its bytes are a header followed by a
// ChaCha8 stream, both derived from genKey and seed, cut to size. genKey is
// the key the content was first written to, which copies keep.
type content struct {
	genKey string
	seed   uint64
	size   int64
	// etag is the ETag the server returned for the content
	etag string
}

func keyHash(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

func (c *content) header() []byte {
	h := make([]byte, headerSize)
	copy(h, magic[:])
	binary.BigEndian.PutUint64(h[4:], c.seed)
	binary.BigEndian.PutUint64(h[12:], uint64(c.size))
	binary.BigEndian.PutUint64(h[20:], keyHash(c.genKey))
	return h
}

// reader returns the content's bytes
func (c *content) reader() io.Reader {
	var seed [32]byte
	binary.BigEndian.PutUint64(seed[:], c.seed)
	binary.BigEndian.PutUint64(seed[8:], keyHash(c.genKey))
	stream := rand.NewChaCha8(seed)
	return io.LimitReader(io.MultiReader(bytes.NewReader(c.header()), stream), c.size)
}

// bytes returns the content's bytes in memory
func (c *content) bytes() []byte {
	b := make([]byte, c.size)
	_, _ = io.ReadFull(c.reader(), b)
	return b
}

// singleETag is the ETag of the content uploaded in one request
func singleETag(data []byte) string {
	sum := sha256.Sum256(data)
	return `"sha256-` + hex.EncodeToString(sum[:]) + `"`
}

// multipartETag is the ETag of the content uploaded as the given parts:
// the SHA-256 of the parts' SHA-256 digests, with the part count
func multipartETag(parts [][]byte) string {
	h := sha256.New()
	for _, p := range parts {
		sum := sha256.Sum256(p)
		h.Write(sum[:])
	}
	return fmt.Sprintf(`"sha256-%s-%d"`, hex.EncodeToString(h.Sum(nil)), len(parts))
}

// verifyRange checks that r holds exactly bytes off to off+n of c, and
// returns an error naming the first difference.
func (c *content) verifyRange(r io.Reader, off, n int64) error {
	want := c.reader()
	if _, err := io.CopyN(io.Discard, want, off); err != nil {
		return fmt.Errorf("generating expected content: %w", err)
	}
	want = io.LimitReader(want, n)

	gotBuf := make([]byte, 64<<10)
	wantBuf := make([]byte, 64<<10)
	var pos int64
	for {
		g, gerr := io.ReadFull(r, gotBuf)
		w, _ := io.ReadFull(want, wantBuf[:max(g, 1)])
		if g > 0 {
			if w < g {
				return fmt.Errorf("body is longer than the %d bytes expected", n)
			}
			if i := firstDiff(gotBuf[:g], wantBuf[:g]); i >= 0 {
				return fmt.Errorf("content differs at byte %d of the range at offset %d", pos+int64(i), off)
			}
			pos += int64(g)
		}
		if gerr == io.EOF || gerr == io.ErrUnexpectedEOF {
			if pos != n {
				return fmt.Errorf("body is %d bytes, want %d", pos, n)
			}
			// The body must end here
			if extra, _ := r.Read(gotBuf[:1]); extra > 0 {
				return fmt.Errorf("body is longer than the %d bytes expected", n)
			}
			return nil
		}
		if gerr != nil {
			return fmt.Errorf("reading body at byte %d: %w", pos, gerr)
		}
	}
}

func firstDiff(a, b []byte) int {
	if bytes.Equal(a, b) {
		return -1
	}
	for i := range a {
		if a[i] != b[i] {
			return i
		}
	}
	return -1
}

// verifySelf checks an object read back from key whose expected content is
// not known: its header must name key and its size, and the rest must be
// the stream the header's seed gives.
func verifySelf(key string, data []byte) error {
	if len(data) < headerSize {
		return fmt.Errorf("object is %d bytes, shorter than the header", len(data))
	}
	if !bytes.Equal(data[:4], magic[:]) {
		return errors.New("object has no header")
	}
	c := &content{
		genKey: key,
		seed:   binary.BigEndian.Uint64(data[4:]),
		size:   int64(binary.BigEndian.Uint64(data[12:])),
	}
	if c.size != int64(len(data)) {
		return fmt.Errorf("header says %d bytes, object is %d", c.size, len(data))
	}
	if binary.BigEndian.Uint64(data[20:]) != keyHash(key) {
		return errors.New("header names another key")
	}
	return c.verifyRange(bytes.NewReader(data), 0, c.size)
}
