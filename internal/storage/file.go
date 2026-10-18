package storage

import (
	"os"
	"syscall"
)

// openFile opens a file like os.OpenFile, with fewer system calls. On
// Linux, os.OpenFile tries to add every file it opens to the network
// poller: it makes the file non-blocking, fails to add it to epoll, which
// does not take regular files, and makes it blocking again. That is five
// system calls on top of the open, for every metadata read, object read
// and temp file. Opening the descriptor with syscall.Open and wrapping it
// with os.NewFile takes one, an fcntl to read its flags, and gives the
// same blocking file that the poller does not handle. Seeking, sendfile
// and copy_file_range work on it as on a file from os.OpenFile.
func openFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	var fd int
	var err error
	for {
		fd, err = syscall.Open(name, flag|syscall.O_CLOEXEC, uint32(perm.Perm()))
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	return os.NewFile(uintptr(fd), name), nil
}

// open opens a file for reading, like os.Open
func open(name string) (*os.File, error) {
	return openFile(name, os.O_RDONLY, 0)
}

// create creates or truncates a file, like os.Create
func create(name string) (*os.File, error) {
	return openFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

// rename renames a file like os.Rename, with one system call less. Before
// renaming, os.Rename lstats the new name to refuse to replace a
// directory, but renaming a file over a directory fails anyway (EISDIR),
// and storage only renames temp files into place.
func rename(oldname, newname string) error {
	var err error
	for {
		err = syscall.Rename(oldname, newname)
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldname, New: newname, Err: err}
	}
	return nil
}
