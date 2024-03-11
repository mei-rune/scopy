package scopy

import (
	"context"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/log"
)

type Session interface {
	io.Closer

	List(remotePath string) ([]fs.FileInfo, error)

	Read(remotePath string) (io.ReadCloser, error)
	Write(remotePath string) (io.WriteCloser, error)
	WriteFile(remotePath string, data []byte) error

	Exists(pa string) (bool, error)
	Rename(from, to string) error
	Delete(pa string) error
}

func fileExists(sess Session, filename string) (bool, error) {
	dir := filepath.Dir(filename)
	if dir == "." {
		dir = ""
	}

	name := filepath.Base(filename)
	list, err := sess.List(dir)
	if err != nil {
		return false, err
	}

	for _, fi := range list {
		if fi.Name() == name {
			return true, nil
		}
	}
	return false, nil
}

func Read(reader io.Reader, maxSize int, fn func(idx int, last bool, data []byte) error) (int, error) {
	var bs = make([]byte, maxSize)
	var count = 0
	var idx = 0
	var offset = 0
	var last = false
	for {
		n, err := reader.Read(bs[offset:])
		if err != nil {
			if err != io.EOF {
				return 0, err
			}
			last = true
		}

		offset += n

		if last || offset >= maxSize {
			err = fn(idx, last, bs[:offset])
			if err != nil {
				return count, err
			}
			if last {
				break
			}

			count += offset
			idx++
		}
	}
	return count, nil
}

func UploadFile(ctx context.Context, sess Session, localPath string, remotePath string) (int64, error) {
	// create destination file
	dstFile, err := sess.Write(remotePath)
	if err != nil {
		return 0, err
	}
	defer dstFile.Close()

	// create source file
	srcFile, err := os.Open(localPath)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	// copy source file to destination file
	bytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		return bytes, err
	}

	return bytes, dstFile.Close()
}

func DownloadFile(ctx context.Context, sess Session, remotePath, localPath string) (int64, error) {
	// create destination file
	dstFile, err := os.Create(localPath)
	if err != nil {
		return 0, err
	}
	defer dstFile.Close()

	// open source file
	srcFile, err := sess.Read(remotePath)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	// copy source file to destination file
	bytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		return bytes, err
	}

	return bytes, dstFile.Close()
}

func DeleteFileIfExists(ctx context.Context, sess Session, remotePath string) (bool, error) {
	err := sess.Delete(remotePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

var DeleteBeforeUpload = true

func UploadDir(ctx context.Context, dir string, sess Session, remoteDir string, deleteAfter bool) error {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return errors.Wrap(err, "枚举本地目录失败")
	}
	logger := log.LoggerOrEmptyFromContext(ctx)

	for _, fi := range fis {
		filename := filepath.Join(dir, fi.Name())
		remoteFile := fi.Name()
		if remoteDir != "" {
			remoteFile = filepath.Join(remoteDir, remoteFile)
		}

		if fi.IsDir() {
			err = UploadDir(ctx, filename, sess, remoteFile, deleteAfter)
			if err != nil {
				return err
			}
		} else {
			remoteFile = filepath.ToSlash(remoteFile)
			if DeleteBeforeUpload {
				if _, err = DeleteFileIfExists(ctx, sess, remoteFile); err != nil {
					return errors.Wrap(err, "上传本地文件 '"+filename+"' 之前先删除， 远程目录 '"+remoteDir+"' 下的同名文件失败")
				}
			}

			_, err = UploadFile(ctx, sess, filename, remoteFile)
			if err != nil {
				return errors.Wrap(err, "上传本地文件 '"+filename+"' 到远程目录 '"+remoteDir+"' 失败")
			}

			logger.Info("上传文件成功", log.String("local", filename), log.String("remote", remoteFile))

			if deleteAfter {
				err = os.Remove(filename)
				if err != nil {
					return errors.Wrap(err, "上传本地文件 '"+filename+"' 后，删除文件失败")
				}
			}
		}
	}
	return nil
}

type ErrDownloadFiles struct {
	Filenames []string
	ErrorList []error
}

func (e *ErrDownloadFiles) Error() string {
	var sb strings.Builder
	sb.WriteString("下载下列文件失败: ")
	for idx := range e.Filenames {
		sb.WriteString(e.Filenames[idx])
		sb.WriteString("(")
		sb.WriteString(e.ErrorList[idx].Error())
		sb.WriteString(")")
	}
	return sb.String()
}

func DownloadDir(ctx context.Context, sess Session, remoteDir string, localDir string, deleteAfter func(remote, local string) bool) error {
	logger := log.LoggerOrEmptyFromContext(ctx)
	return downloadDir(ctx, logger, sess, remoteDir, localDir, deleteAfter)
}

func downloadDir(ctx context.Context, logger log.Logger, sess Session, remoteDir string, localDir string, deleteAfter func(remote, local string) bool) error {
	fis, err := sess.List(remoteDir)
	if err != nil {
		return errors.Wrap(err, "枚举远程目录失败")
	}

	var filenames []string
	var errorList []error

	for _, fi := range fis {
		filename := filepath.Join(localDir, fi.Name())
		remoteFile := fi.Name()
		if remoteDir != "" {
			remoteFile = filepath.Join(remoteDir, remoteFile)
		}

		if fi.IsDir() {
			err = downloadDir(ctx, logger, sess, remoteFile, filename, deleteAfter)
			if err != nil {
				if e, ok := err.(*ErrDownloadFiles); ok {
					filenames = append(filenames, e.Filenames...)
					errorList = append(errorList, e.ErrorList...)
				} else {
					filenames = append(filenames, remoteFile)
					errorList = append(errorList, err)
				}
			}
		} else {
			remoteFile = filepath.ToSlash(remoteFile)

			if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil && !os.IsExist(err) {
				return errors.Wrap(err, "新建本地目录 '"+filepath.Dir(filename)+"' 失败")
			}

			_, err = DownloadFile(ctx, sess, remoteFile, filename)
			if err != nil {
				filenames = append(filenames, remoteFile)
				errorList = append(errorList, err)
				continue
				// return errors.Wrap(err, "下载远程文件 '"+remoteFile+"' 到本地目录 '"+filename+"'  失败")
			}

			logger.Info("下载文件成功", log.String("local", filename), log.String("remote", remoteFile))

			if deleteAfter(remoteFile, filename) {
				err = sess.Delete(remoteFile)
				if err != nil {
					filenames = append(filenames, remoteFile)
					errorList = append(errorList, errors.Wrap(err, "下载后删除失败"))
					continue
					//  return errors.Wrap(err, "下载远程文件 '"+remoteFile+"' 到本地目录后，删除文件失败")
				}
			}
		}
	}

	if len(filenames) != 0 {
		return &ErrDownloadFiles{
			Filenames: filenames,
			ErrorList: errorList,
		}
	}
	return nil
}
