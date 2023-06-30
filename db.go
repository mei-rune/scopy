package scopy

import (
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"strings"
	"time"
)

const (
	DataNone  = 0
	DataStart = -1
	DataEnd   = -2
)

var (
	DefaultResetSQL = `drop table IF EXISTS tpt_files`

	DefaultInitSQL = `CREATE TABLE IF NOT EXISTS tpt_files (
  id                SERIAL PRIMARY KEY,
  uuid              varchar(200) NOT NULL,
  partitioning_count             int,
  partitioning_sequence          int,
  data              bytea,
  created_at        timestamp,

  unique(uuid, partitioning_sequence)
);`
	DefaultInsertSQL       = `insert into tpt_files(uuid, partitioning_count, partitioning_sequence, data, created_at) values(?, ?, ?, ?, now())`
	DefaultReadSQL         = `select id, uuid, partitioning_count, partitioning_sequence, data, created_at from tpt_files limit 1`
	DefaultReadSQLByUUID   = `select id, uuid, partitioning_count, partitioning_sequence, data, created_at from tpt_files where uuid = ? order by partitioning_sequence`
	DefaultRenameSQL       = `update tpt_files set uuid = ? where uuid = ?`
	DefaultDeleteSQLByUUID = `delete from tpt_files where uuid = ?`
	DefaultDeleteSQL       = `delete from tpt_files where id = ?`
	DefaultListSql         = `select fl.uuid as uuid, count(fl.datalength) as length, max(fl.created_at) as created_at from (select tpt_files.uuid as uuid, length(tpt_files.data) as datalength, tpt_files.created_at as created_at from tpt_files) fl group by fl.uuid`
	DefaultExistSql        = `select 1 from tpt_files where uuid = ?`

	DefaultReadDataByID  = `select data from tpt_files where id = ?`
	DefaultReadIDsByUUID = `select id, partitioning_sequence from tpt_files where uuid = ? order by partitioning_sequence`
)

func DB(dbDrv, dbURL, dbTable string, maxSize int) (*dbTarget, error) {
	conn, err := sql.Open(dbDrv, dbURL)
	if err != nil {
		return nil, err
	}
	if maxSize < 1024 {
		maxSize = 10 * 1024 * 1024
	}
	target := &dbTarget{
		conn:    conn,
		maxSize: maxSize,

		insertSql:       DefaultInsertSQL,
		readSqlByUUID:   DefaultReadSQLByUUID,
		renameSql:       DefaultRenameSQL,
		deleteSqlByUUID: DefaultDeleteSQLByUUID,
		listSql:         DefaultListSql,
		existSql:        DefaultExistSql,
	}

	if dbTable != "" {
		target.insertSql = strings.Replace(DefaultInsertSQL, "tpt_files", dbTable, -1)
		target.readSqlByUUID = strings.Replace(DefaultReadSQLByUUID, "tpt_files", dbTable, -1)
		target.renameSql = strings.Replace(DefaultRenameSQL, "tpt_files", dbTable, -1)
		target.deleteSqlByUUID = strings.Replace(DefaultDeleteSQLByUUID, "tpt_files", dbTable, -1)
		target.listSql = strings.Replace(DefaultListSql, "tpt_files", dbTable, -1)
		target.existSql = strings.Replace(DefaultExistSql, "tpt_files", dbTable, -1)
	}

	return target, nil
}

type dbTarget struct {
	conn *sql.DB

	maxSize int

	insertSql       string
	readSqlByUUID   string
	renameSql       string
	deleteSqlByUUID string
	listSql         string
	existSql        string
}

func (st *dbTarget) Close() error {
	return st.conn.Close()
}

type dbFileWriter struct {
	st *dbTarget
	tx *sql.Tx

	uuid string
	idx  int

	isCommited bool
	lastError  error
	buffer     []byte
}

func (w *dbFileWriter) Close() error {
	if w.lastError == nil {
		if len(w.buffer) > 0 {
			w.lastError = w.write(true, w.buffer)
			if w.lastError == nil {
				w.buffer = w.buffer[:0]
			}
		}
	}

	if w.lastError != nil {
		if w.lastError == fs.ErrClosed {
			return nil
		}

		if w.tx != nil {
			err := w.tx.Rollback()
			if err != nil {
				return joinError(w.lastError, err)
			}
			w.tx = nil
		}
		return w.lastError
	}

	if w.tx != nil {
		w.lastError = w.tx.Commit()
		if w.lastError != nil {
			return w.lastError
		}
		w.tx = nil
	}
	w.lastError = fs.ErrClosed
	return nil
}

func (w *dbFileWriter) Write(data []byte) (int, error) {
	if w.lastError != nil {
		return 0, w.lastError
	}
	if len(data) == 0 {
		return 0, nil
	}

	if len(w.buffer) > w.st.maxSize {
		w.lastError = w.write(false, w.buffer)
		if w.lastError != nil {
			return 0, w.lastError
		}
		w.buffer = w.buffer[:0]
	}

	w.buffer = append(w.buffer, data...)
	return len(data), nil
}

func (w *dbFileWriter) write(last bool, data []byte) error {
	total := DataNone
	if w.idx == 0 {
		if last {
			total = DataNone
		} else {
			total = DataStart
		}
	} else if !last {
		total = DataEnd
	}

	var err error
	retried := false

retry:

	if w.tx != nil {
		_, err = w.tx.Exec(w.st.insertSql, w.uuid, total, w.idx, data)
	} else {
		_, err = w.st.conn.Exec(w.st.insertSql, w.uuid, total, w.idx, data)
	}
	if err != nil {
		if strings.Contains(err.Error(), "Error 1062") {
			if w.tx != nil {
				_, err = w.tx.Exec(w.st.deleteSqlByUUID, w.uuid)
			} else {
				_, err = w.st.conn.Exec(w.st.deleteSqlByUUID, w.uuid)
			}
			if err == nil {
				if !retried {
					retried = true
					goto retry
				}
			}
		}

		return err
	}
	w.idx++
	return nil
}

func (w *dbFileWriter) OneWrite(data []byte) error {
	if w.lastError != nil {
		return w.lastError
	}
	if len(w.buffer) > 0 {
		w.lastError = w.write(false, w.buffer)
		if w.lastError != nil {
			return w.lastError
		}
		w.buffer = w.buffer[:0]
	}
	w.lastError = w.write(true, data)
	return w.lastError
}

func (st *dbTarget) Write(remotePath string) (io.WriteCloser, error) {
	tx, err := st.conn.Begin()
	if err != nil {
		return nil, err
	}

	return &dbFileWriter{
		st:   st,
		tx:   tx,
		uuid: remotePath,
		idx:  0,
	}, nil
}

func (st *dbTarget) WriteFile(remotePath string, data []byte) (reterr error) {
	w, err := st.Write(remotePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := w.Close(); err != nil {
			reterr = err
		}
	}()
	return w.(*dbFileWriter).OneWrite(data)
}

type dbFileReader struct {
	rows *sql.Rows

	lastErr error
	lastIdx int
	buffer  []byte
	data    []byte
}

func (r *dbFileReader) Close() error {
	err := r.rows.Close()
	return err
}

func (r *dbFileReader) Read(data []byte) (int, error) {
	if r.lastErr != nil {
		return 0, r.lastErr
	}

	if len(data) == 0 {
		return 0, nil
	}

	for len(r.data) == 0 {
		if !r.rows.Next() {
			r.lastErr = r.rows.Err()
			if r.lastErr != nil {
				return 0, r.lastErr
			}
			r.lastErr = io.EOF
			return 0, io.EOF
		}

		var id int64
		var uuid string
		var total sql.NullInt64
		var idx int
		var dbdata = r.buffer
		var created sql.NullTime
		if len(dbdata) > 0 {
			dbdata = dbdata[:0]
		}

		r.lastErr = r.rows.Scan(&id, &uuid, &total, &idx, &dbdata, &created)
		if r.lastErr != nil {
			return 0, r.lastErr
		}

		if idx != r.lastIdx+1 {
			return 0, errors.New("index sequence is error")
		}
		r.lastIdx = idx

		r.data = dbdata
	}

	if len(r.data) == 0 {
		return 0, io.EOF
	}

	if len(r.data) > len(data) {
		n := copy(data, r.data[:len(data)])
		r.data = r.data[n:]
		return n, nil
	}

	n := copy(data, r.data)
	r.data = r.data[n:]
	return n, nil
}

func (st *dbTarget) Read(remotePath string) (io.ReadCloser, error) {
	rows, err := st.conn.Query(st.readSqlByUUID, remotePath)
	if err != nil {
		return nil, err
	}
	// defer rows.Close()

	return &dbFileReader{
		rows:    rows,
		lastIdx: -1,

		// lastErr error
		// buffer []byte
		// data []byte
	}, nil
}

type fileStat struct {
	name    string
	size    int64
	modTime time.Time
}

func (fs *fileStat) Name() string       { return fs.name }
func (fs *fileStat) IsDir() bool        { return false }
func (fs *fileStat) Size() int64        { return fs.size }
func (fs *fileStat) Mode() fs.FileMode  { return 0 }
func (fs *fileStat) ModTime() time.Time { return fs.modTime }
func (fs *fileStat) Sys() interface{}   { return nil }

func (st *dbTarget) List(remotePath string) ([]fs.FileInfo, error) {
	if remotePath != "" {
		return nil, errors.New("remotePath must is empty")
	}

	rows, err := st.conn.Query(st.listSql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []fs.FileInfo
	for rows.Next() {
		var uuid string
		var length sql.NullInt64
		var created sql.NullTime

		err = rows.Scan(&uuid, &length, &created)
		if err != nil {
			return nil, err
		}

		if !length.Valid {
			length.Int64 = -1
		}
		list = append(list, &fileStat{
			name:    uuid,
			size:    length.Int64,
			modTime: created.Time,
		})
	}
	return list, rows.Err()
}

func (st *dbTarget) Exists(pa string) (bool, error) {
	var count = 0

	err := st.conn.QueryRow(st.existSql, pa).Scan(&count)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return count > 0, nil
}

func (st *dbTarget) Rename(from, to string) error {
	_, err := st.conn.Exec(st.renameSql, to, from)
	return err
}

func (st *dbTarget) Delete(remotePath string) error {
	_, err := st.conn.Exec(st.deleteSqlByUUID, remotePath)
	return err
}

func joinError(err1, err2 error) error {
	if err1 == nil {
		return err2
	}
	if err2 == nil {
		return err1
	}

	return errors.New(err1.Error() + "; " + err2.Error())
}
