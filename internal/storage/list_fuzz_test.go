package storage

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// listFuzzAlphabet is the alphabet of the keys, prefixes, delimiters and
// markers of FuzzListObjects. It is tiny so that prefixes and delimiters
// collide often. "-" sorts before "/", and "é" (two bytes) after "z".
var listFuzzAlphabet = []string{"a", "b", "/", "-", "é", "z"}

// listFuzzDelimiters are the delimiters FuzzListObjects picks from, "/" most
// often
var listFuzzDelimiters = []string{"/", "/", "/", "/", "", "-", "a/", "é"}

// listFuzzInput decodes the fuzz input deterministically, reading zeros past
// its end
type listFuzzInput struct {
	data []byte
}

func (in *listFuzzInput) byte() byte {
	if len(in.data) == 0 {
		return 0
	}
	b := in.data[0]
	in.data = in.data[1:]
	return b
}

func (in *listFuzzInput) str(maxLen int) string {
	n := int(in.byte()) % (maxLen + 1)
	var sb strings.Builder
	for range n {
		sb.WriteString(listFuzzAlphabet[int(in.byte())%len(listFuzzAlphabet)])
	}
	return sb.String()
}

// listModel is the reference listing: the keys after startKey, and not under
// skipPrefix if it is not empty, that start with prefix, with the keys whose
// remainder after the prefix contains the delimiter rolled up into one
// common prefix each, in byte order. keys must be sorted.
func listModel(keys []string, prefix, delimiter, startKey, skipPrefix string) []string {
	var out []string
	for _, k := range keys {
		if k <= startKey || !strings.HasPrefix(k, prefix) {
			continue
		}
		if skipPrefix != "" && strings.HasPrefix(k, skipPrefix) {
			continue
		}
		if delimiter != "" {
			rest := k[len(prefix):]
			if i := strings.Index(rest, delimiter); i >= 0 {
				cp := prefix + rest[:i+len(delimiter)]
				if len(out) == 0 || out[len(out)-1] != cp {
					out = append(out, cp)
				}
				continue
			}
		}
		out = append(out, k)
	}
	return out
}

// isCommonPrefix reports whether s, as a V1 marker, is a common prefix of
// the listing of prefix and delimiter: the prefix, then a remainder whose
// first delimiter ends it
func isCommonPrefix(s, prefix, delimiter string) bool {
	rest, ok := strings.CutPrefix(s, prefix)
	if !ok || delimiter == "" {
		return false
	}
	i := strings.Index(rest, delimiter)
	return i >= 0 && i+len(delimiter) == len(rest)
}

// listPages pages through a listing, V2 with continuation tokens or V1 with
// markers, checks every page and returns the entries of all pages in order
func listPages(t *testing.T, storage *FilesystemStorage, opts ListObjectsOptions, v1 bool, sizes map[string]int) []string {
	t.Helper()
	var all []string
	for page := 0; ; page++ {
		if page > 100 {
			t.Fatalf("listing (v1=%v) did not finish after %d pages: %q", v1, page, all)
		}
		result, err := storage.ListObjects(testBucket, opts)
		if err != nil {
			t.Fatalf("ListObjects(%+v) failed: %v", opts, err)
		}
		var entries []string
		for _, obj := range result.Objects {
			if obj.Size != int64(sizes[obj.Key]) {
				t.Errorf("object %q has size %d, want %d", obj.Key, obj.Size, sizes[obj.Key])
			}
			entries = append(entries, obj.Key)
		}
		if !slices.IsSorted(entries) || !slices.IsSorted(result.CommonPrefixes) {
			t.Fatalf("page %d (v1=%v) is not sorted: objects %q, prefixes %q", page, v1, entries, result.CommonPrefixes)
		}
		// Objects and common prefixes are listed in one order, which is
		// byte order since no object key starts with a common prefix
		entries = append(entries, result.CommonPrefixes...)
		slices.Sort(entries)
		if len(entries) > opts.MaxKeys {
			t.Fatalf("page %d (v1=%v) has %d entries, more than MaxKeys %d: %q", page, v1, len(entries), opts.MaxKeys, entries)
		}
		all = append(all, entries...)
		if !result.IsTruncated {
			return all
		}
		if len(entries) == 0 {
			t.Fatalf("page %d (v1=%v) is truncated but empty", page, v1)
		}
		if last := entries[len(entries)-1]; result.NextMarker != last {
			t.Fatalf("page %d (v1=%v) has NextMarker %q, want its last entry %q", page, v1, result.NextMarker, last)
		}
		if v1 {
			opts.Marker = result.NextMarker
		} else {
			if result.NextContinuationToken == "" {
				t.Fatalf("page %d is truncated without a continuation token", page)
			}
			opts.ContinuationToken = result.NextContinuationToken
		}
	}
}

// encodeListFuzzSeed encodes a FuzzListObjects input: flags (2: delete
// some keys, 4: reopen the storage, 8: build the index before the writes),
// the keys, which of them to delete, the prefix, the delimiter, the marker
// and MaxKeys. Strings must be made of listFuzzAlphabet.
func encodeListFuzzSeed(flags byte, keys, deleted []string, prefix string, delimiter int, marker string, maxKeys int) []byte {
	str := func(s string) []byte {
		b := []byte{byte(len([]rune(s)))}
		for _, r := range s {
			b = append(b, byte(slices.Index(listFuzzAlphabet, string(r))))
		}
		return b
	}
	b := []byte{flags, byte(len(keys))}
	for _, k := range keys {
		b = append(b, str(k)...)
	}
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	for _, k := range slices.Compact(sorted) {
		if slices.Contains(deleted, k) {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
	}
	if flags&2 == 0 {
		b = b[:len(b)-len(slices.Compact(sorted))]
	}
	b = append(b, str(prefix)...)
	b = append(b, byte(delimiter))
	if marker == "" {
		b = append(b, 0)
	} else {
		b = append(b, 1)
		b = append(b, str(marker)...)
	}
	return append(b, byte(maxKeys-1))
}

// FuzzListObjects compares the listings of FilesystemStorage, paged with
// continuation tokens (V2) and with markers (V1), with a reference model.
//
// Run with: go test -run '^$' -fuzz '^FuzzListObjects$' -fuzztime 60s ./internal/storage/
func FuzzListObjects(f *testing.F) {
	type seed struct {
		flags     byte
		keys      []string
		deleted   []string
		prefix    string
		delimiter int // index in listFuzzDelimiters
		marker    string
		maxKeys   int
	}
	for _, sd := range []seed{
		// Nested prefixes
		{0, []string{"a", "a/", "a/a", "a/a/b", "a/b", "a/b/z", "a-", "b", "b/a"}, nil, "a/", 0, "", 2},
		// Delimiter at the end of keys, a key equal to the prefix
		{0, []string{"a", "a/", "a//", "a/b", "b/", "b"}, nil, "a", 0, "", 1},
		// Unicode, deletions and reopen
		{6, []string{"é", "é/", "é/a", "éa", "b", "z", "z/é", "a/é/b"}, []string{"é/a", "z"}, "", 7, "a", 3},
		// A marker that is a common prefix, the index built before the writes
		{8, []string{"a/", "a/a", "a/b", "a-b", "b/a", "b/b", "ba"}, nil, "", 0, "a/", 1},
		// A marker inside a common prefix
		{0, []string{"a/a", "a/b", "a/z", "b"}, nil, "", 0, "a/a", 1},
		// Multi-character delimiter
		{0, []string{"a/a/b", "aa/", "ba/z", "b/a", "a-a/"}, nil, "", 6, "", 2},
		// No delimiter, a marker before the prefix
		{2, []string{"a", "ab", "abz", "b", "ba"}, []string{"abz"}, "a", 4, "-", 2},
	} {
		f.Add(encodeListFuzzSeed(sd.flags, sd.keys, sd.deleted, sd.prefix, sd.delimiter, sd.marker, sd.maxKeys))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		in := &listFuzzInput{data: data}
		flags := in.byte()
		deleteSome := flags&2 != 0
		reopenStorage := flags&4 != 0
		indexFirst := flags&8 != 0

		storage, cleanup := setupTestStorage(t)
		defer cleanup()
		if indexFirst {
			listAll(t, storage, ListObjectsOptions{}) // builds the index, kept up to date by the writes
		}

		sizes := map[string]int{}
		n := int(in.byte()) % 21
		for range n {
			key := in.str(6)
			if ValidateKey(key) != nil {
				continue
			}
			content := key + fmt.Sprint(len(sizes))
			put(t, storage, key, content)
			sizes[key] = len(content)
		}
		keys := make([]string, 0, len(sizes))
		for k := range sizes {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		if deleteSome {
			kept := keys[:0]
			for _, k := range keys {
				if in.byte()&1 != 0 {
					if err := storage.DeleteObject(testBucket, k); err != nil {
						t.Fatalf("DeleteObject(%q) failed: %v", k, err)
					}
					delete(sizes, k)
					continue
				}
				kept = append(kept, k)
			}
			keys = kept
		}

		prefix := in.str(3)
		delimiter := listFuzzDelimiters[int(in.byte())%len(listFuzzDelimiters)]
		marker := ""
		if in.byte()%3 != 0 {
			marker = in.str(4)
		}
		maxKeys := 1 + int(in.byte())%5

		storages := []*FilesystemStorage{storage}
		if reopenStorage {
			storages = append(storages, reopen(t, storage))
		}
		for i, s := range storages {
			// V2: StartAfter, then continuation tokens
			want := listModel(keys, prefix, delimiter, marker, "")
			opts := ListObjectsOptions{Prefix: prefix, Delimiter: delimiter, MaxKeys: maxKeys, StartAfter: marker}
			if got := listPages(t, s, opts, false, sizes); !slices.Equal(got, want) {
				t.Fatalf("storage %d V2 listing of %q with prefix %q, delimiter %q, start-after %q, max-keys %d:\n got %q\nwant %q",
					i, keys, prefix, delimiter, marker, maxKeys, got, want)
			}

			// V1: Marker, then NextMarker. A marker that is a common prefix
			// skips the keys under it, as NextMarker may be one.
			skip := ""
			if isCommonPrefix(marker, prefix, delimiter) {
				skip = marker
			}
			want = listModel(keys, prefix, delimiter, marker, skip)
			opts = ListObjectsOptions{Prefix: prefix, Delimiter: delimiter, MaxKeys: maxKeys, Marker: marker}
			if got := listPages(t, s, opts, true, sizes); !slices.Equal(got, want) {
				t.Fatalf("storage %d V1 listing of %q with prefix %q, delimiter %q, marker %q, max-keys %d:\n got %q\nwant %q",
					i, keys, prefix, delimiter, marker, maxKeys, got, want)
			}
		}
	})
}
