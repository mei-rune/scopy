package scopy

import (
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func OS(dir string) *osTarget {
	return &osTarget{
		dir: dir,
	}
}

type osTarget struct {
	dir string
}

func (st *osTarget) Close() error {
	return nil
}

func (st *osTarget) Write(remotePath string) (io.WriteCloser, error) {
	// create destination file
	return os.Create(filepath.Join(st.dir, remotePath))
}

func (st *osTarget) WriteFile(remotePath string, data []byte) error {
	return ioutil.WriteFile(filepath.Join(st.dir, remotePath), data, 0666)
}

func (st *osTarget) Read(remotePath string) (io.ReadCloser, error) {
	// open source file
	return os.Open(filepath.Join(st.dir, remotePath))
}

func (st *osTarget) List(remotePath string) ([]fs.FileInfo, error) {
	list, err := os.ReadDir(filepath.Join(st.dir, remotePath))
	if err != nil {
		return nil, err
	}

	var results = make([]fs.FileInfo, len(list))
	for idx := range list {
		info, err := list[idx].Info()
		if err != nil {
			return nil, err
		}
		results[idx] = info
	}
	return results, nil
}

func (st *osTarget) Rename(from, to string) error {
	return os.Rename(filepath.Join(st.dir, from), filepath.Join(st.dir, to))
}

func (st *osTarget) Delete(pa string) error {
	log.Println("delete file", filepath.Join(st.dir, pa))
	return os.Remove(filepath.Join(st.dir, pa))
}

func (st *osTarget) Exists(pa string) (bool, error) {
	s, err := os.Stat(filepath.Join(st.dir, pa))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return s != nil, nil
}
