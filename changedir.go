package scopy

import (
	"io"
	"io/fs"
	"path"
)

func Changedir(sess Session, dir string) Session {
  if dir == "" {
    return sess
  }
  return changedirSession{
    Session: sess,
    dir: dir,
  }
}

type changedirSession struct {
	Session
	dir string
}

func (ds changedirSession) Close() error {
	return ds.Session.Close()
}

func (ds changedirSession) List(remotePath string) ([]fs.FileInfo, error) {
	return ds.Session.List(path.Join(ds.dir, remotePath))
}

func (ds changedirSession) Read(remotePath string) (io.ReadCloser, error) {
	return ds.Session.Read(path.Join(ds.dir, remotePath))
}

func (ds changedirSession) Write(remotePath string) (io.WriteCloser, error) {
	return ds.Session.Write(path.Join(ds.dir, remotePath))
}

func (ds changedirSession) WriteFile(remotePath string, data []byte) error {
	return ds.Session.WriteFile(path.Join(ds.dir, remotePath), data)
}

func (ds changedirSession) Exists(remotePath string) (bool, error) {
	return ds.Session.Exists(path.Join(ds.dir, remotePath))
}

func (ds changedirSession) Rename(from, to string) error {
	return ds.Session.Rename(path.Join(ds.dir, from), path.Join(ds.dir, from))
}

func (ds changedirSession) Delete(remotePath string) error {
	return ds.Session.Delete(path.Join(ds.dir, remotePath))
}
