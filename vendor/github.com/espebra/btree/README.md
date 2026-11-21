# BTree implementation for Go

[![Test](https://github.com/espebra/btree/actions/workflows/test.yml/badge.svg)](https://github.com/espebra/btree/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/espebra/btree.svg)](https://pkg.go.dev/github.com/espebra/btree)
[![Go Version](https://img.shields.io/github/go-mod/go-version/espebra/btree)](go.mod)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

This package provides an in-memory B-Tree implementation for Go, useful as
an ordered, mutable data structure.

The API is based off of the wonderful
http://godoc.org/github.com/petar/GoLLRB/llrb, and is meant to allow btree to
act as a drop-in replacement for gollrb trees.

This is a fork of https://github.com/google/btree.

See https://pkg.go.dev/github.com/espebra/btree for documentation.

## Releasing

Releases are cut by pushing a tag of the form `vX.Y.Z` on a commit that is on
`master`:

```sh
git checkout master && git pull
git tag -a v1.2.3 -m v1.2.3
git push origin v1.2.3
```

The [Release workflow](.github/workflows/release.yml) then checks the tag,
runs the full test workflow on the tagged commit, and, if everything passes,
creates a GitHub release with generated notes and publishes the version to the
Go module proxy. Tags with pre-release suffixes (`-rc.1`) are rejected, and
v2 and later need the module path in `go.mod` to end in `/v2` and so on, as Go
requires.
