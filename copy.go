package scopy

import (
	"io"
	stdlog "log"
	"path/filepath"
)

func Upload(sess Session, currentdir string) Target {
	return &UploadCopyer{
		Session:    sess,
		CurrentDir: currentdir,
	}
}

func Download(sess Session, currentdir string) Target {
	return &DownloadCopyer{
		Session:    sess,
		CurrentDir: currentdir,
	}
}

type Target interface {
	io.Closer

	Copy(srcPath, destPath string) error
}

type UploadCopyer struct {
	Session    Session
	CurrentDir string
}

func (cp *UploadCopyer) Close() error {
	return cp.Session.Close()
}

func (cp *UploadCopyer) Copy(srcPath, destPath string) error {
	remotePath := destPath
	if cp.CurrentDir != "" {
		if !filepath.IsAbs(destPath) {
			remotePath = filepath.Join(cp.CurrentDir, destPath)
		}
	}
	remotePath = filepath.ToSlash(remotePath)
	_, err := UploadFile(nil, cp.Session, srcPath, remotePath)
	if err == nil {
		stdlog.Println("copy", srcPath, "to", remotePath)
	}
	return err
}

type DownloadCopyer struct {
	Session    Session
	CurrentDir string
}

func (cp *DownloadCopyer) Close() error {
	return cp.Session.Close()
}

func (cp *DownloadCopyer) Copy(srcPath, destPath string) error {
	remotePath := srcPath
	if cp.CurrentDir != "" {
		if !filepath.IsAbs(srcPath) {
			remotePath = filepath.Join(cp.CurrentDir, srcPath)
		}
	}
	remotePath = filepath.ToSlash(remotePath)
	_, err := DownloadFile(nil, cp.Session, remotePath, destPath)
	if err == nil {
		stdlog.Println("copy", remotePath, "to", srcPath)
	}
	return err
}
