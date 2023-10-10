package scopy

import (
	"io"
	"io/fs"
	"io/ioutil"

	"github.com/pkg/sftp"
	"github.com/runner-mei/errors"
	"github.com/mei-rune/goutils/shell"
	"golang.org/x/crypto/ssh"
)

func SFTPWithKey(host, username, keyfile, passphrase string) (Session, error) {
	var privateKey string
	bs, err := ioutil.ReadFile(keyfile)
	if err != nil {
		return nil, errors.Wrap(err, "load keyfile fail")
	}
	privateKey = string(bs)

	conn, err := shell.DialSSH(host, username, passphrase, privateKey)
	if err != nil {
		return nil, err
	}

	// create new SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &sftpTarget{
		conn:   conn,
		client: client,
	}, nil
}

func SFTPWithPassword(host, username, password string) (Session, error) {
	conn, err := shell.DialSSH(host, username, password, "")
	if err != nil {
		return nil, err
	}

	// create new SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &sftpTarget{
		conn:   conn,
		client: client,
	}, nil
}

type sftpTarget struct {
	conn   *ssh.Client
	client *sftp.Client
}

func (st *sftpTarget) Close() error {
	err1 := st.client.Close()
	err2 := st.conn.Close()

	if err1 == nil {
		return err2
	}
	if err2 == nil {
		return err1
	}
	return errors.ErrArray(err1, err2)
}

func (st *sftpTarget) Write(remotePath string) (io.WriteCloser, error) {
	// create destination file
	return st.client.Create(remotePath)
}

func (st *sftpTarget) WriteFile(remotePath string, data []byte) error {
	w, err := st.Write(remotePath)
	if err != nil {
		return err
	}
	defer w.Close()

	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return nil
		}
		data = data[n:]
	}
	return w.Close()
}

func (st *sftpTarget) Read(remotePath string) (io.ReadCloser, error) {
	// open source file
	return st.client.Open(remotePath)
}

func (st *sftpTarget) List(remotePath string) ([]fs.FileInfo, error) {
	return st.client.ReadDir(remotePath)
}

func (st *sftpTarget) Rename(from, to string) error {
	return st.client.Rename(from, to)
}

func (st *sftpTarget) Delete(path string) error {
	return st.client.Remove(path)
}

func (st *sftpTarget) Exists(pa string) (bool, error) {
	return fileExists(st, pa)
}
