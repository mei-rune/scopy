package scopy

import (
	"net/url"
	"strconv"
	"strings"
  "errors"

  "github.com/xo/dburl"
)

func Open(urlstr, username, password string) (Session, string, error) {
	var remoteDir string
	var sess Session

	if strings.HasPrefix(urlstr, "db+http://") {
		urlstr = strings.TrimPrefix(urlstr, "db+")

		u, err := url.Parse(urlstr)
		if err != nil {
			return nil, "", errWrap(err, "解析 url 失败")
		}

		queryParams := u.Query()
		u.RawQuery = ""

		dbname := queryParams.Get("sc_dbname")
		dbTable := queryParams.Get("sc_dbtable")
    maxSize, _ := strconv.Atoi(queryParams.Get("sc_max_size"))

		sess, err = DBHTTP(u.String(), dbname, username, password, dbTable, maxSize)
		if err != nil {
			return nil, "", errWrap(err, "连接失败")
		}
	} else if strings.HasPrefix(urlstr, "db+") {
		urlstr = strings.TrimPrefix(urlstr, "db+")

    v, err := url.Parse(urlstr)
    if err != nil {
      return nil, "", errWrap(err, "解析 url 失败")
    }
    v.User = url.UserPassword(username, password)

		u, err := dburl.Parse(v.String())
		if err != nil {
			return nil, "", err
		}
		queryParams := u.URL.Query()
		dbTable := queryParams.Get("sc_dbtable")
		maxSize, _ := strconv.Atoi(queryParams.Get("sc_max_size"))

		sess, err = DB(u.Driver, u.DSN, dbTable, maxSize)
		if err != nil {
			return nil, "", errWrap(err, "连接失败")
		}
	} else {
		u, err := url.Parse(urlstr)
		if err != nil {
			return nil, "", errWrap(err, "解析 url 失败")
		}

		switch u.Scheme {
		case "ftp":
			epsv := u.Query().Get("epsv")
			disableEPSV := epsv == "false"
			sess, err = FTP(u.Host, username, password, u.Path, disableEPSV)
		case "sftp", "ssh":
			remoteDir = u.Path
			sess, err = SFTPWithPassword(u.Host, username, password)
		default:
			return nil, "", errors.New("目录不支持")
		}
	}
	return sess, remoteDir, nil
}

func errWrap(err error, msg string) error {
  return errors.New(msg + ": "+ err.Error())
}