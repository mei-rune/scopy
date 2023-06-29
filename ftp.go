package scopy

import (
	"bytes"
	"io"
	"io/fs"

	"github.com/runner-mei/ftp"
)

func FTP(host, username, password, currentdir string, disableEPSV bool) (Session, error) {
	conn, err := ftp.Dial(host)
	if err != nil {
		return nil, err
	}

	if err := conn.Login(username, password); err != nil {
		conn.Quit()
		return nil, err
	}
	if currentdir != "" {
		if err := conn.ChangeDir(currentdir); err != nil {
			conn.Quit()
			return nil, err
		}
	}
	conn.DisableEPSV = disableEPSV

	return &ftpTarget{
		client: conn,
	}, nil
}

type ftpTarget struct {
	client *ftp.ServerConn
}

func (st *ftpTarget) Close() error {
	return st.client.Quit()
}

func (st *ftpTarget) Write(remotePath string) (io.WriteCloser, error) {
	// create destination file
	return st.client.WriteFrom(remotePath, 0)
}

func (st *ftpTarget) WriteFile(remotePath string, data []byte) error {
	err := st.client.Stor(remotePath, bytes.NewReader(data))
	if err != nil {
		return err
	}
	return nil
}

func (st *ftpTarget) Read(remotePath string) (io.ReadCloser, error) {
	// open source file
	return st.client.Retr(remotePath)
}

func (st *ftpTarget) List(remotePath string) ([]fs.FileInfo, error) {
	entries, err := st.client.List(remotePath)
	if err != nil {
		return nil, err
	}
	var list = make([]fs.FileInfo, len(entries))
	for idx := range entries {
		list[idx] = entries[idx].ToFileInfo()
	}
	return list, nil
}

func (st *ftpTarget) Rename(from, to string) error {
	return st.client.Rename(from, to)
}

func (st *ftpTarget) Delete(path string) error {
	return st.client.Delete(path)
}

func (st *ftpTarget) Exists(pa string) (bool, error) {
	return fileExists(st, pa)
}
