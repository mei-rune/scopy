package scopy

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strconv"
	"strings"
	"time"

	aceql_http "github.com/runner-mei/aceql-http-go"
)

func DBHTTP(baseURL, dbname, username, password, dbTable string, maxSize int) (*sqlhttpTarget, error) {
	var c = &aceql_http.Client{
		BaseURL:       baseURL,
		LobAutoUpload: true,
	}

	target := &sqlhttpTarget{
		c: c,

		maxSize:      maxSize,
		dbname:       dbname,
		username:     username,
		password:     password,
		dataAsBinary: true,

		insertSql:       DefaultInsertSQL,
		readSqlByUUID:   DefaultReadSQLByUUID,
		renameSql:       DefaultRenameSQL,
		deleteSqlByUUID: DefaultDeleteSQLByUUID,
		listSql:         DefaultListSql,
		existSql:        DefaultExistSql,
		readDataSql:     DefaultReadDataByID,
		readIDsByUUID:   DefaultReadIDsByUUID,
	}

	if dbTable != "" {
		target.insertSql = strings.Replace(DefaultInsertSQL, "tpt_files", dbTable, -1)
		target.readSqlByUUID = strings.Replace(DefaultReadSQLByUUID, "tpt_files", dbTable, -1)
		target.renameSql = strings.Replace(DefaultRenameSQL, "tpt_files", dbTable, -1)
		target.deleteSqlByUUID = strings.Replace(DefaultDeleteSQLByUUID, "tpt_files", dbTable, -1)
		target.listSql = strings.Replace(DefaultListSql, "tpt_files", dbTable, -1)
		target.existSql = strings.Replace(DefaultExistSql, "tpt_files", dbTable, -1)
		target.readDataSql = strings.Replace(DefaultReadDataByID, "tpt_files", dbTable, -1)
		target.readIDsByUUID = strings.Replace(DefaultReadIDsByUUID, "tpt_files", dbTable, -1)
	}
	return target, nil
}

type sqlhttpTarget struct {
	c *aceql_http.Client

	dbname, username, password string
	maxSize                    int
	dataAsBinary               bool

	insertSql       string
	readSqlByUUID   string
	renameSql       string
	deleteSqlByUUID string
	listSql         string
	existSql        string
	readDataSql     string
	readIDsByUUID   string

	sess *aceql_http.Session
}

func (st *sqlhttpTarget) Close() error {
	return nil
}

func (st *sqlhttpTarget) GetSession() (*aceql_http.Session, error) {
	if st.sess != nil {
		return st.sess, nil
	}

	loginRes, err := st.c.Login(st.dbname, st.username, st.password)
	if err != nil {
		return nil, err
	}

	st.sess = &loginRes.Session
	return st.sess, nil
}

type sqlhttpFileWriter struct {
	st   *sqlhttpTarget
	tx   *sql.Tx
	uuid string
	idx  int

	isCommited bool
	lastError  error
	buffer     []byte
}

func (w *sqlhttpFileWriter) Close() error {
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

func (w *sqlhttpFileWriter) Write(data []byte) (int, error) {
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

func (w *sqlhttpFileWriter) write(last bool, data []byte) error {
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

	sess, err := w.st.GetSession()
	if err != nil {
		return err
	}

	var dataValue = aceql_http.ParamValue{
		Type:  aceql_http.VARCHAR,
		Value: string(data),
	}
	if w.st.dataAsBinary {
		dataValue.Type = aceql_http.BLOB
	} else if len(data) > 10*1024 {
		dataValue.Type = aceql_http.BLOB
	}

	_, err = w.st.c.ExecuteUpdate(sess, w.st.insertSql, []aceql_http.ParamValue{
		{
			Type:  aceql_http.VARCHAR,
			Value: w.uuid,
		},
		{
			Type:  aceql_http.INTEGER,
			Value: strconv.Itoa(total),
		},
		{
			Type:  aceql_http.INTEGER,
			Value: strconv.Itoa(w.idx),
		},
		dataValue,
	}, true)
	if err != nil {
		return err
	}
	w.idx++
	return nil
}

func (w *sqlhttpFileWriter) OneWrite(data []byte) error {
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

func (st *sqlhttpTarget) Write(remotePath string) (io.WriteCloser, error) {
	// tx, err := st.conn.Begin()
	// if err != nil {
	// 	return nil, err
	// }

	return &sqlhttpFileWriter{
		st: st,
		// tx:      tx,
		// maxSize: st.maxSize,
		uuid: remotePath,
		idx:  0,
	}, nil
}

func (st *sqlhttpTarget) WriteFile(remotePath string, data []byte) (reterr error) {
	w, err := st.Write(remotePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := w.Close(); err != nil {
			reterr = err
		}
	}()
	return w.(*sqlhttpFileWriter).OneWrite(data)
}

type sqlhttpFileReader struct {
	st *sqlhttpTarget

	idList  []string
	next    []string
	data    []byte
	lastErr error
}

func (r *sqlhttpFileReader) Close() error {
	return nil
}

func (r *sqlhttpFileReader) Read(data []byte) (int, error) {
	if r.lastErr != nil {
		return 0, r.lastErr
	}

	if len(data) == 0 {
		return 0, nil
	}

	for len(r.data) == 0 {
		if len(r.next) == 0 {
			return 0, io.EOF
		}

		r.data, r.lastErr = r.read(r.next[0])
		if r.lastErr != nil {
			return 0, r.lastErr
		}
		r.next = r.next[1:]
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

func (r *sqlhttpFileReader) read(id string) ([]byte, error) {
	sess, err := r.st.GetSession()
	if err != nil {
		return nil, err
	}

	// args := []aceql_http.ParamValue{
	// 	{
	// 		Type:  aceql_http.INTEGER,
	// 		Value: id,
	// 	},
	// }

	sqlstr := strings.Replace(r.st.readDataSql, "?", id, 1)
	qr, err := r.st.c.ExecuteQuery(sess, sqlstr, nil, true)
	if err != nil {
		return nil, err
	}

	if len(qr.QueryRows) == 0 {
		return nil, errors.New("scopy: result set is empty")
	}

	record := qr.QueryRows[0]["row_1"]
	if record == nil {
		return nil, errors.New("scopy: rows is empty")
	}

	if len(record) == 0 {
		return nil, errors.New("scopy: columns is empty")
	}

	data := record[0]["data"]
	if data == nil {
		return nil, errors.New("scopy: column 'data' is missing")
	}
	s, ok := data.(string)
	if !ok {
		return nil, errors.New("scopy: column 'data' isnot string")
	}

	if len(qr.QueryTypes) > 0 {
		if qr.QueryTypes[0] == aceql_http.CLOB ||
			qr.QueryTypes[0] == aceql_http.BLOB ||
			qr.QueryTypes[0] == aceql_http.BINARY {
			return r.st.c.GetBlob(sess, s)
		}
	}

	return []byte(s), nil
}

func (st *sqlhttpTarget) Read(remotePath string) (io.ReadCloser, error) {
	sess, err := st.GetSession()
	if err != nil {
		return nil, err
	}

	sqlstr := strings.Replace(st.readIDsByUUID, "?", "'"+remotePath+"'", 1)
	// args := []aceql_http.ParamValue{
	// 	{
	// 		Type:  aceql_http.VARCHAR,
	// 		Value: remotePath,
	// 	},
	// }

	results, err := st.c.ExecuteQuery(sess, sqlstr, nil, true)
	if err != nil {
		return nil, err
	}

	selectResults := results.ToSelectResult()

	// if selectResults.Status != aceql_http.OK {
	// 	return nil, results
	// }

	if len(selectResults.ResultSets) == 0 {
		return nil, errors.New("scopy: result set is empty")
	}

	var idList []string
	for idx := range selectResults.ResultSets[0].Rows {
		id := selectResults.ResultSets[0].Rows[idx][0].Value
		idList = append(idList, fmt.Sprint(id))
	}

	// defer rows.Close()

	return &sqlhttpFileReader{
		st: st,

		idList: idList,
		next:   idList,
	}, nil
}

func (st *sqlhttpTarget) List(remotePath string) ([]fs.FileInfo, error) {
	if remotePath != "" {
		return nil, errors.New("remotePath must is empty")
	}
	sess, err := st.GetSession()
	if err != nil {
		return nil, err
	}
	results, err := st.c.ExecuteQuery(sess, st.listSql, nil, true)
	if err != nil {
		return nil, err
	}
	selectResults := results.ToSelectResult()

	// if selectResults.Status != aceql_http.OK {
	// 	return nil, results
	// }

	if len(selectResults.ResultSets) == 0 {
		return nil, errors.New("scopy: result set is empty")
	}

	var list []fs.FileInfo
	for idx := range selectResults.ResultSets[0].Rows {
		values := selectResults.ResultSets[0].Rows[idx]

		var uuid string
		var length sql.NullInt64
		var created sql.NullTime
		for _, value := range values {
			switch value.Name {
			case "uuid":
				uuid = fmt.Sprint(value.Value)
			case "length":
				s := fmt.Sprint(value.Value)
				i64, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return nil, errors.New("scopy: colum 'length' is invalid value '" + s + "'")
				}
				length.Valid = true
				length.Int64 = i64
			case "created_at":
				s := fmt.Sprint(value.Value)
				t, err := ToDatetime(s)
				if err != nil {
					return nil, errors.New("scopy: colum 'created_at' is invalid value '" + s + "'")
				}
				created.Valid = true
				created.Time = t
			default:
				s := fmt.Sprint(value.Value)
				return nil, errors.New("scopy: colum '" + value.Name + "' is invalid value '" + s + "'")
			}
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

	return list, nil
}

func (st *sqlhttpTarget) Exists(pa string) (bool, error) {
	return fileExists(st, pa)
}

func (st *sqlhttpTarget) Rename(from, to string) error {
	sess, err := st.GetSession()
	if err != nil {
		return err
	}

	_, err = st.c.ExecuteUpdate(sess, st.renameSql, []aceql_http.ParamValue{
		{
			Type:  aceql_http.VARCHAR,
			Value: to,
		},
		{
			Type:  aceql_http.VARCHAR,
			Value: from,
		},
	}, true)
	return err
}

func (st *sqlhttpTarget) Delete(remotePath string) error {
	sess, err := st.GetSession()
	if err != nil {
		return err
	}

	_, err = st.c.ExecuteUpdate(sess, st.deleteSqlByUUID, []aceql_http.ParamValue{
		{
			Type:  aceql_http.VARCHAR,
			Value: remotePath,
		},
	}, true)
	return err
}

var (
	TimeFormats = []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-1-_2T15:04:05",
		"2006-1-_2 15:04:05.999999999Z07:00",
		"2006-1-_2 15:04:05Z07:00",
		"2006-1-_2 15:04:05",
		"2006-1-_2",
		"2006/1/_2 15:04:05Z07:00",
		"2006/1/_2 15:04:05",
		"2006/1/_2",
		"2006-01-02T15:04:05 07:00",
	}
	TimeLocation = time.Local
)

func ToDatetime(s string) (time.Time, error) {
	i64, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return time.UnixMilli(i64), nil
	}
	for _, format := range TimeFormats {
		t, err := time.ParseInLocation(format, s, TimeLocation)
		if err == nil {
			return t, nil
		}
	}

	// if strings.HasPrefix(s, "now()") {
	// 	s = strings.TrimPrefix(s, "now()")
	// 	s = strings.TrimSpace(s)

	// 	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
	// 		hasMinus := true
	// 		if strings.HasPrefix(s, "+") {
	// 			hasMinus = false
	// 			s = strings.TrimPrefix(s, "+")
	// 		} else {
	// 			s = strings.TrimPrefix(s, "-")
	// 		}
	// 		s = strings.TrimSpace(s)

	// 		duration, err := ParseDuration(s)
	// 		if err != nil {
	// 			return time.Time{}, errors.New("'" + s + "' isnot duration")
	// 		}
	// 		if hasMinus {
	// 			return time.Now().Add(-duration), nil
	// 		}
	// 		return time.Now().Add(duration), nil
	// 	} else if s == "" {
	// 		return time.Now(), nil
	// 	}
	// }

	return time.Time{}, errors.New("'" + s + "' isnot datetime")
}
